from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import sasl
from cloudera.thrift_sasl import TSaslClientTransport

from .TCLIService import TCLIService

from .cursor import Cursor
from .TCLIService.ttypes import TCloseSessionReq,TOpenSessionReq
AUTH_MECHANISMS = frozenset(['NOSASL', 'PLAIN', 'KERBEROS', 'LDAP'])

class Connection(object):
    DEFAULT_KRB_SERVICE = 'hive'
    client = None
    session = None

    def __init__(self, host=None, port=10000, authMechanism=None, user=None, password=None, database=None, configuration=None, timeout=None):
        if authMechanism not in AUTH_MECHANISMS:
            err_msg = 'authMechanism is not one of the supported ones: [%s]' % ','.join(repr(a) for a in AUTH_MECHANISMS)
            raise NotImplementedError(err_msg)
        #Must set a password for thrift, even if it doesn't need one
        #Open issue with python-sasl
        if authMechanism == 'PLAIN' and (password is None or len(password) == 0):
            password = 'password'
        socket = TSocket(host, port)
        socket.setTimeout(timeout)
        if authMechanism == 'NOSASL':
            transport = TBufferedTransport(socket)
        else:
            sasl_mech = 'PLAIN'
            saslc = sasl.Client()
            saslc.setAttr("username", user)
            saslc.setAttr("password", password)
            if authMechanism == 'KERBEROS':
                krb_host,krb_service = self._get_krb_settings(host, configuration)
                sasl_mech = 'GSSAPI'
                saslc.setAttr("host", krb_host)
                saslc.setAttr("service", krb_service)

            saslc.init()
            transport = TSaslClientTransport(saslc, sasl_mech, socket)

        self.client = TCLIService.Client(TBinaryProtocol(transport))
        transport.open()
        res = self.client.OpenSession(TOpenSessionReq(username=user, password=password, configuration=configuration))
        self.session = res.sessionHandle
        if database is not None:
            with self.cursor() as cur:
                query = "USE {0}".format(database)
                cur.execute(query) 

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def _get_krb_settings(self, default_host, config):
        host = default_host
        service = self.DEFAULT_KRB_SERVICE

        if config is not None:
            if 'krb_host' in config:
                host = config['krb_host']

            if 'krb_service' in config:
                service = config['krb_service']

        return host, service

    def cursor(self):
        return Cursor(self.client, self.session)

    def close(self):
        req = TCloseSessionReq(sessionHandle=self.session)
        self.client.CloseSession(req)
