FROM python:3.12-alpine3.20

WORKDIR /app

COPY . .

RUN apk add --no-cache \
    gcc \
    libffi-dev \
    musl-dev \
    ffmpeg \
    aria2 \
    make \
    g++ \
    cmake \
    wget \
    unzip \
    nodejs \
    npm \
    openssl-dev \
    python3-dev \
    curl && \
    # Install latest Rust via rustup instead of apk (apk rust is too old)
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable && \
    source "$HOME/.cargo/env" && \
    wget -q https://github.com/axiomatic-systems/Bento4/archive/v1.6.0-639.zip && \
    unzip v1.6.0-639.zip && \
    cd Bento4-1.6.0-639 && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j$(nproc) && \
    cp mp4decrypt /usr/local/bin/ && \
    cd ../.. && \
    rm -rf Bento4-1.6.0-639 v1.6.0-639.zip

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir -r requirements.txt \
    && pip3 install --no-cache-dir -U yt-dlp

CMD ["sh", "-c", "gunicorn app:app --bind 0.0.0.0:${PORT:-8080} & python3 main.py"]
