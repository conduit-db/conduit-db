FROM python:3.10

WORKDIR /opt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/opt/"

RUN pip install --no-cache-dir -r ./conduit_raw/requirements.txt
RUN touch ./conduit_raw/headers.mmap && rm ./conduit_raw/headers.mmap
RUN touch ./conduit_raw/block_headers.mmap && rm ./conduit_raw/block_headers.mmap
#CMD sleep 100000
