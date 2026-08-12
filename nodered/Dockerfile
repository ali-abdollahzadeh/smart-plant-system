FROM nodered/node-red:latest
USER root


COPY package.json /data/package.json
# Install nodes
WORKDIR /data
RUN npm install --unsafe-perm --no-update-notifier --no-fund --only=production
RUN cd /data && npm install


# (optional) copy flows if you want auto-load
COPY data/flows.json /data/flows.json
USER node-red


