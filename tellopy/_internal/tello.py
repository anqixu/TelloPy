import datetime
import socket
import struct
import sys
import threading
import time

from . import crc
from . import logger
from . import event
from . import state
from . import error
from . import video_stream
from . utils import *
from . protocol import *
from . import dispatcher


class Tello(object):
    EVENT_CONNECTED = event.Event('connected')
    EVENT_WIFI = event.Event('wifi')
    EVENT_LIGHT = event.Event('light')
    EVENT_FLIGHT_DATA = event.Event('fligt_data')
    EVENT_LOG = event.Event('log')
    EVENT_TIME = event.Event('time')
    EVENT_VIDEO_FRAME = event.Event('video frame')
    EVENT_VIDEO_DATA = event.Event('video data')
    EVENT_DISCONNECTED = event.Event('disconnected')
    # internal events
    __EVENT_CONN_REQ = event.Event('conn_req')
    __EVENT_CONN_ACK = event.Event('conn_ack')
    __EVENT_TIMEOUT = event.Event('timeout')
    __EVENT_QUIT_REQ = event.Event('quit_req')

    # for backward comaptibility
    CONNECTED_EVENT = EVENT_CONNECTED
    WIFI_EVENT = EVENT_WIFI
    LIGHT_EVENT = EVENT_LIGHT
    FLIGHT_EVENT = EVENT_FLIGHT_DATA
    LOG_EVENT = EVENT_LOG
    TIME_EVENT = EVENT_TIME
    VIDEO_FRAME_EVENT = EVENT_VIDEO_FRAME

    STATE_DISCONNECTED = state.State('disconnected')
    STATE_CONNECTING = state.State('connecting')
    STATE_CONNECTED = state.State('connected')
    STATE_QUIT = state.State('quit')

    LOG_ERROR = logger.LOG_ERROR
    LOG_WARN = logger.LOG_WARN
    LOG_INFO = logger.LOG_INFO
    LOG_DEBUG = logger.LOG_DEBUG
    LOG_ALL = logger.LOG_ALL

    def __init__(self,
                 local_cmd_client_port=9000,
                 local_vid_server_port=6038,
                 tello_ip='192.168.0.1',
                 tello_cmd_server_port=8889,
                 log=None):
        self.tello_addr = (tello_ip, tello_cmd_server_port)
        self.debug = False
        self.pkt_seq_num = 0x01e4
        self.local_cmd_client_port = local_cmd_client_port
        self.local_vid_server_port = local_vid_server_port
        self.udpsize = 2000
        self.left_x = 0.0
        self.left_y = 0.0
        self.right_x = 0.0
        self.right_y = 0.0
        self.sock = None
        self.state = self.STATE_DISCONNECTED
        self.lock = threading.Lock()
        self.connected = threading.Event()
        self.video_enabled = False
        self.prev_video_data_time = None
        self.video_data_size = 0
        self.video_data_loss = 0
        self.log = log or logger.Logger('Tello')
        self.exposure = 0
        self.video_encoder_rate = 4
        self.video_stream = None

        # Create a UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', self.local_cmd_client_port))
        self.sock.settimeout(2.0)

        dispatcher.connect(self.__state_machine, dispatcher.signal.All)
        self.recv_thread = threading.Thread(target=self.__recv_thread)
        self.video_thread = threading.Thread(target=self.__video_thread)
        self.recv_thread.start()
        self.video_thread.start()

    def set_loglevel(self, level):
        """
        Set_loglevel controls the output messages. Valid levels are
        LOG_ERROR, LOG_WARN, LOG_INFO, LOG_DEBUG and LOG_ALL.
        """
        self.log.set_level(level)

    def get_video_stream(self):
        """
        Get_video_stream is used to prepare buffer object which receive video data from the drone.
        """
        newly_created = False
        self.lock.acquire()
        self.log.info('get video stream')
        try:
            if self.video_stream is None:
                self.video_stream = video_stream.VideoStream(self)
                newly_created = True
            res = self.video_stream
        finally:
            self.lock.release()
        if newly_created:
            self.__send_exposure()
            self.__send_video_encoder_rate()
            self.start_video()

        return res

    def connect(self):
        """Connect is used to send the initial connection request to the drone."""
        self.__publish(event=self.__EVENT_CONN_REQ)

    def wait_for_connection(self, timeout=None):
        """Wait_for_connection will block until the connection is established."""
        if not self.connected.wait(timeout):
            raise error.TelloError('timeout')

    def __send_conn_req(self):
        port_bytes = struct.pack('<H', self.local_vid_server_port)
        buf = 'conn_req:'+port_bytes
        self.log.info('send connection request (cmd="%sx%02xx%02x")' %
                      (str(buf[:-2]), ord(port_bytes[0]), ord(port_bytes[1])))
        return self.send_packet(Packet(buf))

    def subscribe(self, signal, handler):
        """Subscribe a event such as EVENT_CONNECTED, EVENT_FLIGHT_DATA, EVENT_VIDEO_FRAME and so on."""
        dispatcher.connect(handler, signal)

    def __publish(self, event, data=None, **args):
        args.update({'data': data})
        if 'signal' in args:
            del args['signal']
        if 'sender' in args:
            del args['sender']
        self.log.debug('publish signal=%s, args=%s' % (event, args))
        dispatcher.send(event, sender=self, **args)

    def takeoff(self):
        """Takeoff tells the drones to liftoff and start flying."""
        self.log.info('takeoff (cmd=0x%02x seq=0x%04x)' %
                      (TAKEOFF_CMD, self.pkt_seq_num))
        pkt = Packet(TAKEOFF_CMD)
        pkt.fixup()
        return self.send_packet(pkt)

    def throw_takeoff(self):
        """Throw_takeoff starts motors and expects to be thrown to takeoff."""
        self.log.info('throw_takeoff (cmd=0x%02x seq=0x%04x)' %
                      (THROW_TAKEOFF_CMD, self.pkt_seq_num))
        pkt = Packet(THROW_TAKEOFF_CMD)
        pkt.fixup()
        return self.send_packet(pkt)

    def land(self, stop_landing=False):
        """Land tells the drone to come in for landing."""
        self.log.info('land (cmd=0x%02x seq=0x%04x)' %
                      (LAND_CMD, self.pkt_seq_num))
        pkt = Packet(LAND_CMD)
        pkt.add_byte(int(stop_landing))
        pkt.fixup()
        return self.send_packet(pkt)

    def palm_land(self, stop_landing=False):
        """Palm_land tells the drone to cut motors near flat surface below."""
        self.log.info('palm_land (cmd=0x%02x seq=0x%04x)' %
                      (PALM_LAND_CMD, self.pkt_seq_num))
        pkt = Packet(PALM_LAND_CMD)
        pkt.add_byte(int(stop_landing))
        pkt.fixup()
        return self.send_packet(pkt)

    def flattrim(self):
        """FlatTrim re-calibrates IMU to be horizontal."""
        self.log.info('flattrim (cmd=0x%02x seq=0x%04x)' %
                      (FLATTRIM_CMD, self.pkt_seq_num))
        pkt = Packet(FLATTRIM_CMD)
        pkt.fixup()
        return self.send_packet(pkt)

    def quit(self):
        """Quit stops the internal threads."""
        self.log.info('quit')
        self.__publish(event=self.__EVENT_QUIT_REQ)
        self.recv_thread.join()
        self.video_thread.join()

    def __send_time_command(self):
        self.log.info('send_time (cmd=0x%02x seq=0x%04x)' %
                      (TIME_CMD, self.pkt_seq_num))
        pkt = Packet(TIME_CMD, 0x50)
        pkt.add_byte(0)
        pkt.add_time()
        pkt.fixup()
        return self.send_packet(pkt)

    def __send_start_video(self):
        pkt = Packet(VIDEO_START_CMD, 0x60)
        pkt.fixup()
        return self.send_packet(pkt)

    def start_video(self):
        """Start_video tells the drone to send start info (SPS/PPS) for video stream."""
        self.log.info('start video (cmd=0x%02x seq=0x%04x)' %
                      (VIDEO_START_CMD, self.pkt_seq_num))
        self.video_enabled = True
        self.__send_exposure()
        self.__send_video_encoder_rate()
        return self.__send_start_video()

    def set_exposure(self, level):
        """Set_exposure sets the drone camera exposure level. Valid levels are 0, 1, and 2."""
        if level < 0 or 2 < level:
            raise error.TelloError('Invalid exposure level')
        self.log.info('set exposure (cmd=0x%02x seq=0x%04x)' %
                      (EXPOSURE_CMD, self.pkt_seq_num))
        self.exposure = level
        return self.__send_exposure()

    def __send_exposure(self):
        pkt = Packet(EXPOSURE_CMD, 0x48)
        pkt.add_byte(self.exposure)
        pkt.fixup()
        return self.send_packet(pkt)

    def set_video_encoder_rate(self, rate):
        """Set_video_encoder_rate sets the drone video encoder rate."""
        self.log.info('set video encoder rate (cmd=0x%02x seq=%04x)' %
                      (VIDEO_ENCODER_RATE_CMD, self.pkt_seq_num))
        self.video_encoder_rate = rate
        return self.__send_video_encoder_rate()

    def __send_video_encoder_rate(self):
        pkt = Packet(VIDEO_ENCODER_RATE_CMD, 0x68)
        pkt.add_byte(self.video_encoder_rate)
        pkt.fixup()
        return self.send_packet(pkt)

    def flip(self, flip_dir=FLIP_FRONT):
        """flip tells the drone to perform a flip given a protocol.FLIP_XYZ direction"""
        self.log.info('flip (cmd=0x%02x seq=0x%04x)' %
                      (FLIP_CMD, self.pkt_seq_num))
        pkt = Packet(FLIP_CMD, 0x70)
        if flip_dir < 0 or flip_dir >= FLIP_MAX_INT:
            flip_dir = FLIP_FRONT
        pkt.add_byte(flip_dir)
        pkt.fixup()
        return self.send_packet(pkt)

    def __fix_range(self, val, min=-1.0, max=1.0):
        if val < min:
            val = min
        elif val > max:
            val = max
        return val

    def set_vspeed(self, vspeed):
        """
        Set_vspeed controls the vertical up and down motion of the drone.
        Pass in an int from -1.0 ~ 1.0. (positive value means upward)
        """
        if self.left_y != self.__fix_range(vspeed):
            self.log.info('set_vspeed(val=%4.2f)' % vspeed)
        self.left_y = self.__fix_range(vspeed)

    def set_yaw(self, yaw):
        """
        Set_yaw controls the left and right rotation of the drone.
        Pass in an int from -1.0 ~ 1.0. (positive value will make the drone turn to the right)
        """
        if self.left_x != self.__fix_range(yaw):
            self.log.info('set_yaw(val=%4.2f)' % yaw)
        self.left_x = self.__fix_range(yaw)

    def set_pitch(self, pitch):
        """
        Set_pitch controls the forward and backward tilt of the drone.
        Pass in an int from -1.0 ~ 1.0. (positive value will make the drone move forward)
        """
        if self.right_y != self.__fix_range(pitch):
            self.log.info('set_pitch(val=%4.2f)' % pitch)
        self.right_y = self.__fix_range(pitch)

    def set_roll(self, roll):
        """
        Set_roll controls the the side to side tilt of the drone.
        Pass in an int from -1.0 ~ 1.0. (positive value will make the drone move to the right)
        """
        if self.right_x != self.__fix_range(roll):
            self.log.info('set_roll(val=%4.2f)' % roll)
        self.right_x = self.__fix_range(roll)

    def __send_stick_command(self):
        pkt = Packet(STICK_CMD, 0x60)

        axis1 = int(1024 + 660.0 * self.right_x) & 0x7ff
        axis2 = int(1024 + 660.0 * self.right_y) & 0x7ff
        axis3 = int(1024 + 660.0 * self.left_y) & 0x7ff
        axis4 = int(1024 + 660.0 * self.left_x) & 0x7ff
        '''
        11 bits (-1024 ~ +1023) x 4 axis = 44 bits
        44 bits will be packed in to 6 bytes (48 bits)

                    axis4      axis3      axis2      axis1
             |          |          |          |          |
                 4         3         2         1         0
        98765432109876543210987654321098765432109876543210
         |       |       |       |       |       |       |
             byte5   byte4   byte3   byte2   byte1   byte0
        '''
        self.log.debug("stick command: yaw=%4d vrt=%4d pit=%4d rol=%4d" %
                       (axis4, axis3, axis2, axis1))
        # self.log.warn("> %3.1f %3.1f %3.1f %3.1f %4d %4d %4d %4d" % (
        #    self.left_x, self.left_y, self.right_x, self.right_y, axis1, axis2, axis3, axis4)) # TODO: remove debug
        pkt.add_byte(((axis2 << 11 | axis1) >> 0) & 0xff)
        pkt.add_byte(((axis2 << 11 | axis1) >> 8) & 0xff)
        pkt.add_byte(((axis3 << 11 | axis2) >> 5) & 0xff)
        pkt.add_byte(((axis4 << 11 | axis3) >> 2) & 0xff)
        pkt.add_byte(((axis4 << 11 | axis3) >> 10) & 0xff)
        pkt.add_byte(((axis4 << 11 | axis3) >> 18) & 0xff)
        pkt.add_time()
        pkt.fixup()
        self.log.debug("stick command: %s" %
                       byte_to_hexstring(pkt.get_buffer()))
        return self.send_packet(pkt)

    def send_packet(self, pkt):
        """Send_packet is used to send a command packet to the drone."""
        try:
            cmd = pkt.get_buffer()
            self.sock.sendto(cmd, self.tello_addr)
            self.log.debug("send_packet: %s" % byte_to_hexstring(cmd))
        except socket.error as err:
            if self.state == self.STATE_CONNECTED:
                self.log.error("send_packet: %s" % str(err))
            else:
                self.log.info("send_packet: %s" % str(err))
            return False

        return True

    def __process_packet(self, data):
        if isinstance(data, str):
            data = bytearray([x for x in data])

        if str(data[0:9]) == 'conn_ack:' or data[0:9] == b'conn_ack:':
            self.log.info('connected. (vid_port=x%2xx%2x)' %
                          (data[9], data[10]))
            self.log.debug('    %s' % byte_to_hexstring(data))
            if self.video_enabled:
                self.__send_exposure()
                self.__send_video_encoder_rate()
                self.__send_start_video()
            self.__publish(self.__EVENT_CONN_ACK, data)

            return True

        if data[0] != START_OF_PACKET:
            self.log.info('start of packet != %02x (%02x) (ignored)' %
                          (START_OF_PACKET, data[0]))
            self.log.info('    %s' % byte_to_hexstring(data))
            self.log.info('    %s' % str(map(chr, data))[1:-1])
            return False

        pkt = Packet(data)
        cmd = int16(data[5], data[6])
        if cmd == LOG_MSG:
            self.log.debug("recv: log: %s" % byte_to_hexstring(data[9:]))
            self.__publish(event=self.EVENT_LOG, data=data[9:])
        elif cmd == WIFI_MSG:
            self.log.debug("recv: wifi: %s" % byte_to_hexstring(data[9:]))
            self.__publish(event=self.EVENT_WIFI, data=data[9:])
        elif cmd == LIGHT_MSG:
            self.log.debug("recv: light: %s" % byte_to_hexstring(data[9:]))
            self.__publish(event=self.EVENT_LIGHT, data=data[9:])
        elif cmd == FLIGHT_MSG:
            flight_data = FlightData(data[9:])
            self.log.debug("recv: flight data: %s" % str(flight_data))
            self.__publish(event=self.EVENT_FLIGHT_DATA, data=flight_data)
        elif cmd == TIME_CMD:
            self.log.debug("recv: time data: %s" % byte_to_hexstring(data))
            self.__publish(event=self.EVENT_TIME, data=data[7:9])
        elif cmd in (TAKEOFF_CMD, THROW_TAKEOFF_CMD, LAND_CMD, PALM_LAND_CMD, FLATTRIM_CMD, VIDEO_START_CMD, VIDEO_ENCODER_RATE_CMD):
            self.log.info("recv: ack: cmd=0x%02x seq=0x%04x %s" %
                          (int16(data[5], data[6]), int16(data[7], data[8]), byte_to_hexstring(data)))
        else:
            self.log.info('unknown packet: %s' % byte_to_hexstring(data))
            return False

        return True

    def __state_machine(self, event, sender, data, **args):
        self.lock.acquire()
        cur_state = self.state
        event_connected = False
        event_disconnected = False
        self.log.debug('event %s in state %s' % (str(event), str(self.state)))

        if self.state == self.STATE_DISCONNECTED:
            if event == self.__EVENT_CONN_REQ:
                self.__send_conn_req()
                self.state = self.STATE_CONNECTING
            elif event == self.__EVENT_QUIT_REQ:
                self.state = self.STATE_QUIT
                event_disconnected = True
                self.video_enabled = False

        elif self.state == self.STATE_CONNECTING:
            if event == self.__EVENT_CONN_ACK:
                self.state = self.STATE_CONNECTED
                event_connected = True
                # send time
                self.__send_time_command()
            elif event == self.__EVENT_TIMEOUT:
                self.__send_conn_req()
            elif event == self.__EVENT_QUIT_REQ:
                self.state = self.STATE_QUIT

        elif self.state == self.STATE_CONNECTED:
            if event == self.__EVENT_TIMEOUT:
                self.__send_conn_req()
                self.state = self.STATE_CONNECTING
                event_disconnected = True
                self.video_enabled = False
            elif event == self.__EVENT_QUIT_REQ:
                self.state = self.STATE_QUIT
                event_disconnected = True
                self.video_enabled = False

        elif self.state == self.STATE_QUIT:
            pass

        if cur_state != self.state:
            self.log.info('state transit %s -> %s' % (cur_state, self.state))
        self.lock.release()

        if event_connected:
            self.__publish(event=self.EVENT_CONNECTED, **args)
            self.connected.set()
        if event_disconnected:
            self.__publish(event=self.EVENT_DISCONNECTED, **args)
            self.connected.clear()

    def __recv_thread(self):
        sock = self.sock

        while self.state != self.STATE_QUIT:

            if self.state == self.STATE_CONNECTED:
                self.__send_stick_command()  # ignore errors

            try:
                data, server = sock.recvfrom(self.udpsize)
                self.log.debug("recv: %s" % byte_to_hexstring(data))
                self.__process_packet(data)
            except socket.timeout as ex:
                if self.state == self.STATE_CONNECTED:
                    self.log.error('recv: timeout')
                self.__publish(event=self.__EVENT_TIMEOUT)
            except Exception as ex:
                self.log.error('recv: %s' % str(ex))
                show_exception(ex)

        self.log.info('exit from the recv thread.')

    def __video_thread(self):
        self.log.info('start video thread')
        # Create a UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', self.local_vid_server_port))
        sock.settimeout(5.0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 512 * 1024)
        self.log.info('video receive buffer size = %d' %
                      sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF))

        prev_header = None
        prev_ts = None
        history = []
        while self.state != self.STATE_QUIT:
            if not self.video_enabled:
                time.sleep(0.1)
                continue
            try:
                data, server = sock.recvfrom(self.udpsize)
                now = datetime.datetime.now()
                self.log.debug("video recv: %s %d bytes" %
                               (byte_to_hexstring(data[0:2]), len(data)))
                show_history = False

                # check video data loss
                header = byte(data[0])
                if (prev_header is not None and
                    header != prev_header and
                        header != ((prev_header + 1) & 0xff)):
                    loss = header - prev_header
                    if loss < 0:
                        loss = loss + 256
                    self.video_data_loss += loss
                    #
                    # enable this line to see packet history
                    # show_history = True
                    #
                prev_header = header

                # check video data interval
                if prev_ts is not None and 0.1 < (now - prev_ts).total_seconds():
                    self.log.info('video recv: %d bytes %02x%02x +%03d' %
                                  (len(data), byte(data[0]), byte(data[1]),
                                   (now - prev_ts).total_seconds() * 1000))
                prev_ts = now

                # save video data history
                history.append(
                    [now, len(data), byte(data[0])*256 + byte(data[1])])
                if 100 < len(history):
                    history = history[1:]

                # show video data history
                if show_history:
                    prev_ts = history[0][0]
                    for i in range(1, len(history)):
                        [ts, sz, sn] = history[i]
                        print('    %02d:%02d:%02d.%03d %4d bytes %04x +%03d%s' %
                              (ts.hour, ts.minute, ts.second, ts.microsecond/1000,
                               sz, sn, (ts - prev_ts).total_seconds()*1000,
                               (' *' if i == len(history) - 1 else '')))
                        prev_ts = ts
                    history = history[-1:]

                # deliver video frame to subscribers
                self.__publish(event=self.EVENT_VIDEO_FRAME, data=data[2:])
                self.__publish(event=self.EVENT_VIDEO_DATA, data=data)

                # show video frame statistics
                if self.prev_video_data_time is None:
                    self.prev_video_data_time = now
                self.video_data_size += len(data)
                dur = (now - self.prev_video_data_time).total_seconds()
                if 2.0 < dur:
                    self.log.info(('video data %d bytes %5.1fKB/sec' %
                                   (self.video_data_size, self.video_data_size / dur / 1024)) +
                                  ((' loss=%d' % self.video_data_loss) if self.video_data_loss != 0 else ''))
                    self.video_data_size = 0
                    self.prev_video_data_time = now
                    self.video_data_loss = 0

                    # keep sending start video command
                    self.__send_start_video()

            except socket.timeout as ex:
                self.log.error('video recv: timeout')
                data = None
            except Exception as ex:
                self.log.error('video recv: %s' % str(ex))
                show_exception(ex)

        self.log.info('exit from the video thread.')


if __name__ == '__main__':
    print('You can use test.py for testing.')
