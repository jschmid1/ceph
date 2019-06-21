import rados
import json


class Cluster(object):
    # FIXME: proper pathing
    settings = {
        'conf': "/home/jxs/projects/ceph/build/ceph.conf",
        'client': 'client.admin',
    }

    def __init__(self):
        self.cluster = None
        self.configure()
        self.connect()
        # TODO: structure

    def configure(self):
        self.cluster = rados.Rados(
            conffile=self.settings['conf'], name=self.settings['client'])

    @property
    def is_connected(self):
        return self.state == 'connected'

    def connect(self):
        self.cluster.connect()
        if not self.is_connected:
            return False
        return True

    @property
    def state(self):
        return self.cluster.state

    def mark_osd(self, state, osd_ids):
        assert state
        assert osd_ids  # is this neccessary?
        assert isinstance(osd_ids, list)
        cmd = json.dumps({"prefix": f"osd {state}", "ids": f"{osd_ids}"})
        return self.cluster.mon_command(cmd, b'')

    def osd_empty(self, osd_ids):
        return (0, "Dummy return", "foo")

    def purge_osd(self, osd_ids):
        assert osd_ids
        cmd = json.dumps({"prefix": "osd purge", "ids": f"{osd_ids}"})
        return self.cluster.mon_command(cmd, b'')


def mark_osd(state, osd_ids):
    return Cluster().mark_osd(state, osd_ids)


def osd_empty(osd_ids):
    return Cluster().osd_empty(osd_ids)


def purge_osd(osd_ids):
    return Cluster().purge_osd(osd_ids)
