FROM python:3.9

WORKDIR /opt

COPY . .
RUN rm /opt/.env
RUN mv /opt/.env.docker /opt/.env

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/opt/"

RUN pip install --no-cache-dir -r ./conduit_raw/requirements.txt
RUN touch ./conduit_raw/headers.mmap && rm ./conduit_raw/headers.mmap
RUN touch ./conduit_raw/block_headers.mmap && rm ./conduit_raw/block_headers.mmap
#CMD sleep 100000
