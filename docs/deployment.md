
# Deployment

## Backend

systemctl restart direct-ai-webapp.service

## Проверка nginx

nginx -t
systemctl reload nginx

## Проверка backend

curl http://127.0.0.1:8088/health

## Web интерфейс

https://ai.aidaplus.ru
