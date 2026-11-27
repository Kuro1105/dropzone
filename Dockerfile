FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dropzone dropzone
COPY static static
# Run as an unprivileged user; /downloads is the only writable place it needs.
RUN useradd --create-home dz && mkdir /downloads && chown dz /downloads
USER dz
# Inside a container the UI must listen on 0.0.0.0 so Docker can forward to it;
# docker-compose.yml only publishes it on the host's 127.0.0.1.
ENV PYTHONUNBUFFERED=1 DZ_WEB_HOST=0.0.0.0 DZ_DOWNLOADS=/downloads
EXPOSE 8000 50000/udp 50001
CMD ["python", "-m", "dropzone.main"]
