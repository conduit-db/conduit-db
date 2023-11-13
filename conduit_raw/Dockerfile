FROM python_base:latest

WORKDIR /opt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/opt/"

RUN touch ./conduit_raw/headers.mmap && rm ./conduit_raw/headers.mmap
RUN touch ./conduit_raw/block_headers.mmap && rm ./conduit_raw/block_headers.mmap
CMD sleep infinity
