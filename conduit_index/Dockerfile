FROM python:3.10

WORKDIR /opt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/opt/"

RUN apt update
RUN pip install --no-cache-dir -r ./conduit_index/requirements.txt
RUN touch ./conduit_index/headers.mmap && rm ./conduit_index/headers.mmap
RUN touch ./conduit_index/block_headers.mmap && rm ./conduit_index/block_headers.mmap
CMD sleep infinity
