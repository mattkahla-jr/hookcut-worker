FROM python:3.12
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY worker.py /app/worker.py
ENV PYTHONUNBUFFERED=1 HOOKCUT_WORKDIR=/tmp
CMD ["/bin/bash", "-c", "while true; do python3 /app/worker.py; sleep 15; done"]
