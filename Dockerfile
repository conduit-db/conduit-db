FROM python_base:latest

COPY . .

RUN dos2unix ./contrib/run_static_checks.sh
RUN chmod +x ./contrib/run_static_checks.sh

CMD ["./contrib/run_static_checks.sh"]
