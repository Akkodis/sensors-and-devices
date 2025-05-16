from logging import raiseExceptions
import signal
import subprocess
import sys
from datetime import datetime
import time, os, random, json

from proton.handlers import MessagingHandler
from proton.reactor import Container

import content

import discovery_registration

from threading import Thread
import time

sourceTTL = os.getenv('VIDEO_TTL')

keepalive_timeout = 30

class Sender(MessagingHandler):
    def __init__(self, url, message):
        super(Sender, self).__init__()
        self.url = url
        self._message = message
        self._sent_count = 0
        self._confirmed_count = 0

    def on_start(self, event):
        print("Sender Created")
        event.container.create_sender(self.url)

    def on_sendable(self, event):
        message = self._message
        #print("Send to "+ self.url +": \n\t" + str(message))
        print(message)
        event.sender.send(message)
        self._sent_count += 1
        event.sender.close()

    def on_accepted(self, event):
        #print("on_accepted")
        self._confirmed_count += 1
        event.connection.close()

    def on_transport_error(self, event):
        raise Exception(event.transport.condition)


class AMQP:
    th = None

    # Define a function for the thread
    def keep_alive(self):
        while 1:
            time.sleep(keepalive_timeout)
            r = discovery_registration.keepAliveDataflow(self.param['metadata'],self.param['id'])
            self.param['send'] = r.json()['send']
            print(self.param['send'])

    def __init__(self, subscription:list=None, param:dict=None) -> None:
        ptx = None

        self.subscription = subscription
        self.param = param

        # Create Keep alive thread
        try:
            self.th = Thread(target=self.keep_alive, args=())
            self.th.start()
        except:
            print ("\n\n\t\tError: unable to start thread\n\n")
        time.sleep(1)

        # Wait Keep alive response to start video signalling (AMQP), negotiation and sending (WebRTC)
        while(not self.param['send']):
            time.sleep(1)

        # Use ID from the registration
        vid = self.param['id']
        # Send AMQP message to notify WebRTC proxy intention to start sending video
        content.message_generator(vid, self.param['tile'], self.param['metadata'])
        Container(Sender(self.param['serverURL']+self.subscription[0], content.message)).run()
        time.sleep(3)

        # Start WebRTC video negotiation and sending to the peer
        if (self.param['sourceType'] == 'udp'):
            command = './webrtcTX --self-id=%i --peer-id=%s --report-period=0 --disable-ssl --server="ws://%s:%i" --udp=%i' %(vid, "peer"+str(vid), self.param['vserverIP'], int(self.param['vserverPort']), int(self.param['sourcePar']))
            print('command: '+command)      
            ptx = subprocess.Popen(command,shell=True)
        elif (self.param['sourceType'] == 'rtsp'):
            command = './webrtcTX --self-id=%i --peer-id=%s --report-period=0 --disable-ssl --server="ws://%s:%i" --rtsp=%s' %(vid, "peer"+str(vid), self.param['vserverIP'], int(self.param['vserverPort']), self.param['sourcePar'])
            print('command: '+command)      
            ptx = subprocess.Popen(command,shell=True)
        elif (self.param['sourceType'] == 'file'):
            command = './webrtcTX --self-id=%i --peer-id=%s --report-period=0 --disable-ssl --server="ws://%s:%i" --file=%s' %(vid, "peer"+str(vid), self.param['vserverIP'], int(self.param['vserverPort']), self.param['sourcePar'])
            print('command: '+command)      
            ptx = subprocess.Popen(command,shell=True)

        # Send video during the configured period
        time.sleep(int(sourceTTL))

        # Notify intention to stop sending video
        Container(Sender(self.param['serverURL']+self.subscription[1], content.message)).run()
        time.sleep(5)
        try:
            os.killpg(os.getpgid(ptx.pid), signal.SIGINT)
        except KeyboardInterrupt:
            sys.exit()
