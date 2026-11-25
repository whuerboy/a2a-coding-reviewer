# 基于 RocketMQ 的多智能体代码协作审查系统

本项目基于 Apache RocketMQ 5.5.0（Normal Topic + LiteTopic）与 Python 实现多智能体异步代码审查系统，包含 `SupervisorAgent`、`StyleAgent`、`LogicAgent`、`SecurityAgent`、`SummaryAgent` 五个智能体。

## 快速启动（3 步）

### 1. 修改配置
编辑 `.env` 文件，填入你的**真实学号**和 **大模型 API Key**：
```env
STUDENT_ID=你的学号
LLM_API_KEY=你的APIKey
```

### 2. 启动 RocketMQ 5.5.0 集群并初始化资源
```bash
docker compose up -d
# 等待约 15 秒让 Broker 启动完成
chmod +x init_rmq.sh && ./init_rmq.sh
```

### 3. 运行多智能体审查
打开终端 1，安装依赖并启动 4 个 Worker Agent：
```bash
pip install -r requirements.txt
python workers.py
```

打开终端 2，启动 SupervisorAgent 触发审查：
```bash
python supervisor.py
```
直接按回车即可运行内置测试代码，等待几秒后输出完整 Markdown 审查报告。

## 控制台核验
访问 `http://localhost:8082`：
- **Topic / Consumer**：核验带学号的 `rmqai-{学号}-review-task`、`rmqai-{学号}-review-reply` 及 5 个 Consumer Group。
- **Message Trace**：使用 `{学号}-{taskId}` 作为 Message Key 搜索并截图保存完整消息轨迹。
