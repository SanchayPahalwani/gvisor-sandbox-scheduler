FROM python:3.13-slim
RUN pip install --no-cache-dir numpy==2.5.4 pandas==3.1.1
RUN useradd -u 1000 -m sandbox && mkdir -p /work /gss
COPY zygote.py /gss/zygote.py
COPY pyfork /gss/pyfork
RUN chmod 0755 /gss/pyfork /gss/zygote.py
