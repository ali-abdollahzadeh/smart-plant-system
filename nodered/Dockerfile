# 使用官方 Node-RED 基础镜像
FROM nodered/node-red:latest

# 在此添加你想要安装的 Node-RED 模块
# --unsafe-perm 是为了避免权限问题
RUN npm install node-red-contrib-influxdb node-red-dashboard node-red-node-ui-table --unsafe-perm

# 复制你的 flows.json 文件到容器的 /nodered 目录
# 这是 Node-RED 默认存放流程文件的地方
COPY flows.json /nodered/flows.json