FROM python:3.11-bookworm

RUN apt update 
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install uv
RUN uv venv /etc/venv
ENV VIRTUAL_ENV=/etc/venv
ENV PATH="/etc/venv/bin:$PATH"

RUN uv pip install -r /app/requirements.txt

COPY . .

EXPOSE 4444
RUN chmod +x /etc/venv/bin/activate
RUN /etc/venv/bin/activate

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "4444", "--workers", "4"]