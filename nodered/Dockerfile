FROM nodered/node-red:latest

COPY package.json /data/package.json
# Install nodes
RUN cd /data && npm install

# (optional) copy flows if you want auto-load
# COPY data/flows.json /data/flows.json



