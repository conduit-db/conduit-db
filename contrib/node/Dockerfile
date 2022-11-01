FROM python:3.10

# required to flush logs to stdout
ENV PYTHONUNBUFFERED=1
RUN python3 -m pip install electrumsv_node
COPY ./contrib/node /opt
COPY ./contrib/scripts/import_blocks.py /opt
COPY ./contrib/node/bitcoin-cli.sh /

RUN apt-get update
RUN apt-get install dos2unix
RUN dos2unix /bitcoin-cli.sh
RUN chmod +x /bitcoin-cli.sh

EXPOSE 18332
EXPOSE 18444
CMD ["python3", "/opt/server.py"]
