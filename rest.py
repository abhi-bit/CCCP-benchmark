import json
import urllib2
from dcp_bin_client import DCPClient
from mc_bin_client import MemcachedClient


class Rest:
    def __init__(self, ip, admin_port, mcd_port):
        self.ip = ip
        self.admin_port = admin_port
        self.mcd_port = mcd_port
        self.clientStatMap = {}
        self.DCPClientMap = {}
        self.mcdClientMap = {}

    def updateVbMap(self):
        self.__vbServerMap = self.vbServerMap()
        for key in self.DCPClientMap:
            client = self.DCPClientMap[key]
            client.close()
        for key in self.mcdClientMap:
            client = self.mcdClientMap[key]
            client.close()

        self.DCPClientMap = {}
        self.mcdClientMap = {}
        self.clientStatMap = {}

    def vbServerMap(self):
        result = json.loads(urllib2.urlopen(self.url).read())
        vbServerMap = result['vBucketServerMap']
        return vbServerMap

    def serverList(self):
        return self.__vbServerMap['serverList']

    def vbMap(self):
        return self.__vbServerMap['vBucketMap']

    def serverVb(self, vb):
        serverList = self.serverList()
        vbMap = self.vbMap()
        return serverList[vbMap[vb][0]]

    def vbDCPClient(self, vb):
        server = self.ip
        if server in self.DCPClientMap:
            client = self.DCPClientMap[server]
        else:
            client = DCPClient(server, self.mcd_port)
            client.open_producer("dcpstream"+str(vb))
            self.DCPClientMap[server] = client

        return client

    def vbMcdClient(self, vb):
        server = self.serverVb(vb)
        if server in self.mcdClientMap:
            client = self.mcdClientMap[server]
        else:
            ip, port = server.split(':')
            client = MemcachedClient(ip, int(port))
            self.mcdClientMap[server] = client

        return client

    def vbSeqnoUuid(self, vb):
        server = self.serverVb(vb)
        if server in self.clientStatMap:
            stats = self.clientStatMap[server]
        else:
            client = self.vbMcdClient(vb)
            stats = client.stats('vbucket-seqno')
            self.clientStatMap[server] = stats

        seqno = int(stats['vb_%s:high_seqno' % vb])
        uuid = int(stats['vb_%s:uuid' % vb])

        return seqno, uuid
