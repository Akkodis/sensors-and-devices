from __future__ import print_function
from py5gmeta.activemq.amqp import  VideoReceiver
from proton.reactor import Container
import carla
import random
import sys
import gi
from gi.repository import Gst, GObject, GLib, GstApp, GstVideo


pts = 0  # buffers presentation timestamp
gi.require_version('GLib', '2.0')
gi.require_version('GObject', '2.0')
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')


if __name__ == "__main__":

    client = carla.Client("192.168.12.3", 2000)
    client.load_world('Town05')
    world = client.get_world()
    spectator = world.get_spectator()
    transform = spectator.get_transform()
    location = transform.location
    rotation = transform.rotation

    # Set the spectator with an empty transform
    spectator.set_transform(carla.Transform())
    # This will set the spectator at the origin of the map, with 0 degrees
    # pitch, yaw and roll - a good way to orient yourself in the ma
    vehicle_blueprints = world.get_blueprint_library().filter('*vehicle*')
    spawn_points = world.get_map().get_spawn_points()
    for i in range(0, 50):
        world.try_spawn_actor(random.choice(vehicle_blueprints), random.choice(spawn_points))
    ego_vehicle = world.spawn_actor(random.choice(vehicle_blueprints), random.choice(spawn_points))
    # Create a transform to place the camera on top of the vehicle
    camera_init_trans = carla.Transform(carla.Location(z=1.5))
    # We create the camera through a blueprint that defines its properties
    camera_bp = world.get_blueprint_library().find('sensor.camera.rgb')

    # We spawn the camera and attach it to our ego vehicle
    camera = world.spawn_actor(camera_bp, camera_init_trans, attach_to=ego_vehicle)

    appsrc = None

    pipeline = None
    bus = None
    message = None

    # initialize GStreamer
    Gst.init(sys.argv[1:])

    # build the pipeline
    pipeline = Gst.parse_launch(
        'appsrc caps="video/x-h264, stream-format=byte-stream, alignment=au" name=appsrc ! h264parse config-interval=-1 ! decodebin ! videoconvert ! autovideosink'
    )

    appsrc = pipeline.get_by_name("appsrc")  # get AppSrc
    # instructs appsrc that we will be dealing with timed buffer
    appsrc.set_property("format", Gst.Format.TIME)

    # instructs appsrc to block pushing buffers until ones in queue are preprocessed
    # allows to avoid huge queue internal queue size in appsrc
    appsrc.set_property("block", True)

    # start playing
    print("Pipeline Playing")
    ret = pipeline.set_state(Gst.State.PLAYING)
    if ret == Gst.StateChangeReturn.FAILURE:
        print("Unable to set the pipeline to the playing state.")
        exit(-1)

    print("Container RECEIVER")
    Container(VideoReceiver(url)).run()

    # wait until EOS or error
    bus = pipeline.get_bus()

    # free resources
    pipeline.set_state(Gst.State.NULL)
