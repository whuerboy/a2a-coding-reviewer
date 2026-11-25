import os
from dotenv import load_dotenv

load_dotenv()

STUDENT_ID = os.getenv("STUDENT_ID", "2026001")
RMQ_ENDPOINT = os.getenv("RMQ_ENDPOINT", "127.0.0.1:8081")

# Topic 规划
TASK_TOPIC = f"rmqai-{STUDENT_ID}-review-task"    # Normal Topic（任务分发）
REPLY_TOPIC = f"rmqai-{STUDENT_ID}-review-reply"  # Lite 父 Topic（会话级结果回传）

# Consumer Group 规划
GID_STYLE = f"GID_{STUDENT_ID}_style"
GID_LOGIC = f"GID_{STUDENT_ID}_logic"
GID_SECURITY = f"GID_{STUDENT_ID}_security"
GID_SUMMARY = f"GID_{STUDENT_ID}_summary"
GID_SUPERVISOR = f"GID_{STUDENT_ID}_supervisor"

# LLM 配置
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
