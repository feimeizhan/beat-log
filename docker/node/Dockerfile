FROM node:8-alpine 

WORKDIR /www

COPY package*.json ./

RUN apk add --update --virtual build_deps python make g++ && \
    npm install --only=production && \
    apk del build_deps

COPY . .

CMD [ "npm", "start" ]
