# 5GMETA Sensors and Devices

## Introduction

In this page, we introduce examples, including scenario, pre-conditions and scripts/Dockers, to produce video streams.

The examples presented are located in the [examples/video-stream-broker](https://github.com/5gmeta/5gmeta-dev/blob/main/examples/video-stream-broker/) folder.

## Scenario

A video sensor wants to push a UDP video stream including a RTP video with H264

## Pre-Conditions

The following systems must be configured and running:

- The **Message Broker (AMQP)** is up and running
- **The Kakfa Broker (KAFKA)** is up and running and ready at EKS. It polls data through Kafka Connector and KSQLDB from AMQP
- The **GPS positions** in [`examples/video-stream-broker/video_sensor.py`](https://github.com/5gmeta/5gmeta-dev/blob/main/examples/video-stream-broker/video_sensor.py) to get the tiles are hardcoded in the example removing the integration with a specific GPS device.
- The **component** for the **Video Sensor** is **built** but not running

## Usage 

* To Push a UDP Source to 5GMETA MEC 
1.  First, we need a UDP SOURCE or RTSP Stream with an H.264 video

	- ``` $ gst-launch-1.0 videotestsrc is-live=true do-timestamp=true ! videoconvert ! videorate ! video/x-raw, width=640, height=480, framerate=10/1 ! textoverlay font-desc="Arial 40px" text="container TX" valignment=2 ! timeoverlay font-desc="Arial 60px" valignment=2 ! videoconvert ! tee name=t ! queue max-size-buffers=1 ! x264enc bitrate=2000 speed-preset=ultrafast tune=zerolatency key-int-max=5 ! video/x-h264,profile=constrained-baseline,stream-format=byte-stream ! h264parse ! rtph264pay pt=96 config-interval=-1 name=payloader ! application/x-rtp,media=video,encoding-name=H264,payload=96 ! udpsink host=127.0.0.1 port=7000 sync=false enable-last-sample=false send-duplicates=false ```

3.  Second, we need to run the Video Sensor Docker sending the UDP video stream

	- ``` $ docker run  --net=host --env AMQP_USER="<user>" --env AMQP_PASS="<password>" --env VIDEO_SOURCE="udp" --env VIDEO_PARAM="7000" --env VIDEO_TTL="100" 5gmeta/video_sensor ```
 	- _Remember to configure the `<user`>, `<password`> to your local environment_
	- ***NB:***
		- ***Note that **7000** is the **UDP source Port*****
		- ***Note that **100** is the **timeout** until the Video Sensor will stop sending a video stream***
  
4.  Third, we can play the video from the 5GMETA MEC infrastructure, produced by the WebRTC Proxy, allowing data pipelines at the 5GMETA MEC infrastructure to process

	- ``` $ gst-launch-1.0 udpsrc port=5045 ! application/x-rtp,encoding-name=H264,payload=96 ! rtpjitterbuffer latency=50 ! rtph264depay ! decodebin ! videoconvert ! textoverlay font-desc="Arial 40px" text="local RX" valignment=1 ! videoconvert ! autovideosink```
	- ***NB:***
		- ***Note that **5045** is the **UDP Port produced** by the WebRTC proxy based on the provided ID (from the registration process)***


## Local Consume a AMQP Source

1.  First, change [`amqp2video.py`](https://github.com/5gmeta/5gmeta-dev/blob/main/examples/video-stream-broker/amqp2video.py) code to set the **target ID**

	- ``` if (event.message.properties['sourceId'] == 45):```
	- ***NB: Note that **45** should be changed to the **dataflow ID identified** in the logs coming from registration and WebRTC proxy logs***

2. Second, launch the player

	- ``` $ AMQP_USER="<user>" AMQP_PASS="<password>" AMQP_IP="<AAA.BBB.CCC.DDD>" AMQP_PORT="<port>" GST_DEBUG=3 python3 ./amqp2video.py```
 	- _Remember to configure the `<user`>, `<password`>, `<AAA.BBB.CCC.DDD`> and `<port`> to your local environment_




## Deployment of the S&D Modules

In this section, a Sensors&Devices instance is created to send data to the  5GMETA MEC platform though the message  and video brokers hosted in the MEC.


### Prerequisities

- [VirtualBox](https://www.virtualbox.org/)
- [Vagrant](https://developer.hashicorp.com/vagrant/tutorials/getting-started/getting-started-install?product_intent=vagrant)


### Deploy an Instances of a S&D

Installation requirements can be found in the repository. Clone the repository using

```bash
git clone ssh://git@gitlab.akka.eu:22522/5gmeta-new/sensors-and-devices.git
cd sensors-and-devices/vagrant
vagrant up
```

Vagrant will provision the VM with the following  Senders:


- ***cits_sender_python***: a python sender and receiver that run an AMQP sender simulating a number of vehicles that send messages with some properties attached.

- ***image_sender_python***: a python sender and receiver that run an AMQP sender simulating a number of vehicle that sends images (different size, but all the same)

- ***cits_receiver_python***: a python example to receive messages from AMQP events
