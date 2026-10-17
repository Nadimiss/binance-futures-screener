FROM python:3.12-slim
WORKDIR /app
COPY app.py index.html ./
ENV PORT=10000
EXPOSE 10000
CMD ["python", "app.py"]
