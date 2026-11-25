#!/bin/bash
set -e

if [ ! -f .env ]; then
  echo "未找到 .env 文件，请先配置 .env"
  exit 1
fi

set -a
source .env
set +a

if [ -z "$STUDENT_ID" ]; then
  echo "请先在 .env 中填写 STUDENT_ID"
  exit 1
fi

echo "正在为学号 ${STUDENT_ID} 初始化 RocketMQ Topic 与 Consumer Group..."

# 1. 创建 Normal Topic（任务分发）
docker exec rmqbroker sh mqadmin updateTopic -n rmqnamesrv:9876 -c DefaultCluster \
  -t "rmqai-${STUDENT_ID}-review-task" -a +message.type=NORMAL

# 2. 创建 Lite 父 Topic（会话级结果回传）
docker exec rmqbroker sh mqadmin updateTopic -n rmqnamesrv:9876 -c DefaultCluster \
  -t "rmqai-${STUDENT_ID}-review-reply" -a +message.type=LITE

# 3. 创建 5 个独立 Consumer Group
for role in style logic security summary supervisor; do
  docker exec rmqbroker sh mqadmin updateSubGroup -n rmqnamesrv:9876 -c DefaultCluster \
    -g "GID_${STUDENT_ID}_${role}"
done

echo "初始化完成！可访问 http://localhost:8082 查看 Topic 与 Consumer Group。"
