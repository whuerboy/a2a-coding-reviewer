import os, socket, platform
os.environ["COMPUTERNAME"] = "rmq-win-client"
socket.gethostname = lambda: "rmq-win-client"
platform.node = lambda: "rmq-win-client"
import json
import threading
import time
from rocketmq import (
    ClientConfiguration,
    Credentials,
    FilterExpression,
    Message,
    Producer,
    SimpleConsumer,
)
from config import (
    GID_LOGIC,
    GID_SECURITY,
    GID_STYLE,
    GID_SUMMARY,
    REPLY_TOPIC,
    RMQ_ENDPOINT,
    STUDENT_ID,
    TASK_TOPIC,
)
from llm_client import ask_llm


PROMPTS = {
    "style": "你是 StyleAgent，专注检查代码风格、命名规范、可读性与注释。请用简明条目输出问题与修改建议。",
    "logic": "你是 LogicAgent，专注分析潜在逻辑缺陷、空指针、死循环、边界条件与异常处理。请用简明条目输出问题与修复方案。",
    "security": "你是 SecurityAgent，专注识别安全漏洞（SQL注入、XSS、硬编码密钥、命令注入等）。请指出风险等级与修复代码。",
    "summary": "你是 SummaryAgent。请将 Style、Logic、Security 三个维度的审查意见去重、按严重级别（高/中/低）排序，生成一份结构化的 Markdown Code Review 报告。",
}


def make_config() -> ClientConfiguration:
    return ClientConfiguration(RMQ_ENDPOINT, Credentials())


def send_reply(producer: Producer, session_id: str, task_id: str, role: str, result_text: str):
    msg = Message()
    msg.topic = REPLY_TOPIC
    msg.tag = role
    # 消息 Key 带学号，便于控制台消息轨迹溯源
    msg.keys = f"{STUDENT_ID}-{task_id}-{role}"
    # 写入会话级 LiteTopic（兼容不同版本 SDK 属性名）
    if hasattr(msg, "lite_topic"):
        msg.lite_topic = session_id
    else:
        msg.message_group = session_id

    payload = {
        "session_id": session_id,
        "task_id": task_id,
        "role": role,
        "result": result_text,
    }
    msg.body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    receipt = producer.send(msg)
    print(f"[{role.upper()}Agent] 已回传结果 -> LiteTopic={session_id}, MsgId={receipt.message_id}")


def run_worker(role: str, group_id: str, subscribe_tag: str):
    cfg = make_config()
    consumer = SimpleConsumer(cfg, group_id)
    consumer.startup()
    consumer.subscribe(TASK_TOPIC, FilterExpression(subscribe_tag))

    producer = Producer(cfg, (REPLY_TOPIC,))
    producer.startup()

    print(f"[{role.upper()}Agent] 启动成功 | Group={group_id} | 监听 Topic={TASK_TOPIC} (Tag={subscribe_tag})")

    try:
        while True:
            messages = consumer.receive(max_message_num=4, invisible_duration=30)
            for msg in messages:
                try:
                    data = json.loads(msg.body.decode("utf-8"))
                    session_id = data["session_id"]
                    task_id = data["task_id"]
                    content = data["content"]

                    print(f"[{role.upper()}Agent] 收到任务 task_id={task_id}，正在调用 LLM 分析...")
                    review_result = ask_llm(PROMPTS[role], content)
                    send_reply(producer, session_id, task_id, role, review_result)
                    consumer.ack(msg)
                except Exception as e:
                    print(f"[{role.upper()}Agent] 处理消息失败: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.shutdown()
        producer.shutdown()


if __name__ == "__main__":
    # Style / Logic / Security 并行消费 Tag=REVIEW，实现广播式并行审查
    # Summary 消费 Tag=SUMMARY，负责最终汇总
    agents = [
        ("style", GID_STYLE, "REVIEW"),
        ("logic", GID_LOGIC, "REVIEW"),
        ("security", GID_SECURITY, "REVIEW"),
        ("summary", GID_SUMMARY, "SUMMARY"),
    ]
    threads = []
    for role, gid, tag in agents:
        t = threading.Thread(target=run_worker, args=(role, gid, tag), daemon=True)
        t.start()
        threads.append(t)

    print("\n所有 Worker Agent 已就绪，按 Ctrl+C 退出...\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n正在关闭所有 Worker Agent...")
