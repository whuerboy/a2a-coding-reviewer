import os, socket, platform
os.environ["COMPUTERNAME"] = "rmq-win-client"
socket.gethostname = lambda: "rmq-win-client"
platform.node = lambda: "rmq-win-client"
import json
import time
import uuid
from rocketmq import (
    ClientConfiguration,
    Credentials,
    FilterExpression,
    Message,
    Producer,
    SimpleConsumer,
)
from config import (
    GID_SUPERVISOR,
    REPLY_TOPIC,
    RMQ_ENDPOINT,
    STUDENT_ID,
    TASK_TOPIC,
)
# 内置一段包含风格、逻辑和 SQL 注入漏洞的测试代码
SAMPLE_CODE = """
def getUser(uid, db):
    # 拼接 SQL，存在注入风险；且未校验 uid 是否为空
    q = "SELECT * FROM users WHERE id = '" + uid + "'"
    res = db.execute(q).fetchall()
    return res[0]  # 若查询为空会触发 IndexError
"""


def run_review(code_snippet: str):
    cfg = ClientConfiguration(RMQ_ENDPOINT, Credentials())

    # 1. 生成带学号前缀的会话 ID（作为 LiteTopic 标识）
    task_id = uuid.uuid4().hex[:8]
    session_id = f"{STUDENT_ID}-session-{task_id}"

    producer = Producer(cfg, (TASK_TOPIC,))
    producer.startup()

    consumer = SimpleConsumer(cfg, GID_SUPERVISOR)
    consumer.startup()
    consumer.subscribe(REPLY_TOPIC, FilterExpression("*"))

    try:
        # 2. 发送代码审查任务到 Normal Topic（Tag=REVIEW，3个 Worker 并行消费）
        task_msg = Message()
        task_msg.topic = TASK_TOPIC
        task_msg.tag = "REVIEW"
        task_msg.keys = f"{STUDENT_ID}-{task_id}-supervisor-dispatch"
        task_msg.body = json.dumps(
            {"session_id": session_id, "task_id": task_id, "content": code_snippet},
            ensure_ascii=False,
        ).encode("utf-8")

        receipt = producer.send(task_msg)
        print(f"\n[Supervisor] 任务已分发 -> Topic={TASK_TOPIC} | Session={session_id} | MsgId={receipt.message_id}")
        print("[Supervisor] 等待 Style / Logic / Security 三个 Agent 并行回传结果...\n")

        # 3. 从 LiteTopic 收集 3 个维度的结果
        collected = {}
        deadline = time.time() + 120
        while len(collected) < 3 and time.time() < deadline:
            msgs = consumer.receive(max_message_num=8, invisible_duration=30)
            for m in msgs:
                data = json.loads(m.body.decode("utf-8"))
                if data.get("session_id") == session_id and data.get("role") in ("style", "logic", "security"):
                    role = data["role"]
                    collected[role] = data["result"]
                    print(f"[Supervisor] 已收到 {role.upper()}Agent 结果 ({len(collected)}/3)")
                    consumer.ack(m)

        if len(collected) < 3:
            print("[Supervisor] 等待超时，未收齐全部 Agent 结果。")
            return

        # 4. 将 3 份结果发给 SummaryAgent 汇总（Tag=SUMMARY）
        print("\n[Supervisor] 3 份审查结果已收齐，正在交由 SummaryAgent 生成最终报告...")
        merged_text = (
            f"### StyleAgent 意见\n{collected['style']}\n\n"
            f"### LogicAgent 意见\n{collected['logic']}\n\n"
            f"### SecurityAgent 意见\n{collected['security']}"
        )
        sum_msg = Message()
        sum_msg.topic = TASK_TOPIC
        sum_msg.tag = "SUMMARY"
        sum_msg.keys = f"{STUDENT_ID}-{task_id}-supervisor-summary"
        sum_msg.body = json.dumps(
            {"session_id": session_id, "task_id": task_id, "content": merged_text},
            ensure_ascii=False,
        ).encode("utf-8")
        producer.send(sum_msg)

        # 5. 等待 SummaryAgent 回传最终报告
        final_report = None
        deadline = time.time() + 120
        while not final_report and time.time() < deadline:
            msgs = consumer.receive(max_message_num=4, invisible_duration=30)
            for m in msgs:
                data = json.loads(m.body.decode("utf-8"))
                if data.get("session_id") == session_id and data.get("role") == "summary":
                    final_report = data["result"]
                    consumer.ack(m)
                    break

        print("\n" + "=" * 60)
        print("最终代码审查报告 (SummaryAgent 生成)")
        print("=" * 60)
        print(final_report or "汇总超时")
        print("=" * 60)
        print(f"\n请打开 http://localhost:8082 -> Message Trace，搜索 Key 前缀: {STUDENT_ID}-{task_id} 进行截图。")

    finally:
        producer.shutdown()
        consumer.shutdown()


if __name__ == "__main__":
    print("直接按回车使用内置测试代码，或粘贴你的代码后按回车：")
    user_input = input("> ").strip()
    run_review(user_input if user_input else SAMPLE_CODE)
