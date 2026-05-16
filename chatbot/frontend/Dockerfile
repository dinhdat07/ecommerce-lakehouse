FROM node:24-alpine AS build

WORKDIR /app
COPY chatbot/frontend/package.json /app/package.json
COPY chatbot/frontend/tsconfig.json /app/tsconfig.json
COPY chatbot/frontend/tsconfig.app.json /app/tsconfig.app.json
COPY chatbot/frontend/vite.config.ts /app/vite.config.ts
COPY chatbot/frontend/postcss.config.js /app/postcss.config.js
COPY chatbot/frontend/tailwind.config.js /app/tailwind.config.js
COPY chatbot/frontend/index.html /app/index.html
COPY chatbot/frontend/src /app/src
RUN npm install && npm run build

FROM nginx:1.27-alpine

COPY chatbot/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
