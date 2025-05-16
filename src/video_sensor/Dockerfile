
FROM ubuntu:20.04

# Disable Prompt During Packages Installation
ARG DEBIAN_FRONTEND=noninteractive

# Update Ubuntu Software repository
RUN apt update
RUN apt install -y python3
RUN apt install -y python3-pip
RUN pip3 install -Iv meson==0.53.2
RUN apt install -y ninja-build

RUN apt update
RUN apt install -y libsoup2.4-1
RUN apt install -y libsoup2.4-dev
RUN apt install -y libssl1.1
RUN apt install -y libssl-dev
RUN apt install -y libsrtp2-1
RUN apt install -y libsrtp2-dev
RUN apt install -y libgnutls30
RUN apt install -y libgnutls28-dev
RUN apt install -y libgnutls-openssl27
RUN apt install -y libnice10
RUN apt install -y libnice-dev
RUN apt install -y libwebsockets15
RUN apt install -y libwebsockets-dev
RUN apt install -y flex
RUN apt install -y bison

RUN apt update
RUN apt install -y iputils-ping
RUN apt install -y libpango-1.0-0
RUN apt install -y libpango1.0-dev
RUN apt install -y libpango-1.0-0
RUN apt install -y libpango1.0-dev
RUN apt install -y libpangocairo-1.0-0
RUN apt install -y libjson-glib-1.0-0
RUN apt install -y libjson-glib-dev

RUN apt update
RUN apt install -y libavfilter7
RUN apt install -y libavfilter-dev
RUN apt install -y libavformat58
RUN apt install -y libavformat-dev
RUN apt install -y libavresample4
RUN apt install -y libavresample-dev
RUN apt install -y libavcodec58
RUN apt install -y libavcodec-dev
RUN apt install -y libavutil56
RUN apt install -y libavutil-dev

RUN apt install -y libtool
RUN apt install -y git

RUN git clone https://github.com/sctplab/usrsctp
WORKDIR usrsctp
RUN git checkout c1d6cb3bf4344e01e7d0b3157f4e7103d55b0fbf
RUN chmod +x bootstrap
RUN ./bootstrap
RUN ./configure --prefix /usr/
RUN make
RUN make install

RUN update-ca-certificates
RUN git config --global http.sslverify false

RUN git clone --branch 1.18 https://github.com/GStreamer/gst-build.git
WORKDIR gst-build
RUN mkdir build
RUN meson --prefix=/usr build
RUN ninja -C build
COPY icestream.c ./gst-plugins-bad/ext/webrtc
RUN ninja -C build
RUN meson install -C build

RUN pip3 install --user websockets

RUN pip3 install websockets
RUN pip3 install pygeotile requests

ENV protocol --none

WORKDIR /opt/
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY video_sensor.py amqp_manager.py content.py discovery_registration.py security.py webrtcTX ./
RUN touch videoamqp.log

ENTRYPOINT ["python3"]

CMD ["video_sensor.py"]
