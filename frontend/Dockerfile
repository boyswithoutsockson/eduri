FROM node:lts
WORKDIR /app

COPY package.json /app/
RUN npm install 
RUN npm run build

CMD node 


