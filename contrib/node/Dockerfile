FROM electrumsvsdk/base_image:latest

# required to flush logs to stdout
ENV PYTHONUNBUFFERED=1

RUN python3 -m pip install electrumsv_node

COPY ./contrib/node /opt

EXPOSE 18332
CMD ["python3", "/opt/server.py"]
