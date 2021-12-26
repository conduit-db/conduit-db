FROM python:3.9

WORKDIR /opt

COPY . .
RUN rm /opt/.env
RUN mv /opt/.env.docker /opt/.env

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/opt/"

RUN apt update
RUN apt install gdb -y
RUN pip install --no-cache-dir -r ./conduit_index/requirements.txt
RUN touch ./conduit_index/headers.mmap && rm ./conduit_index/headers.mmap
RUN touch ./conduit_index/block_headers.mmap && rm ./conduit_index/block_headers.mmap
CMD sleep 100000
