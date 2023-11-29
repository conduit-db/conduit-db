FROM python_base:latest

COPY . .

RUN chmod +x ./contrib/run_static_checks.sh

CMD ["./contrib/run_static_checks.sh"]
