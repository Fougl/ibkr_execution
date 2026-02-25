from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time

class IBAPI(EWrapper, EClient):
    def __init__(self, host, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.host = host
        self.port = port
        self.client_id = client_id

        self.connected_flag = False
        self.nextOrderId = None
        self.position_data = {}

    # -------------------------------------
    # CONNECTION
    # -------------------------------------
    def connect_and_start(self):
        super().connect(self.host, self.port, self.client_id)

        # reader thread
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()

        # wait connection
        for _ in range(100):
            if self.connected_flag:
                break
            time.sleep(0.05)

        return self.connected_flag

    def nextValidId(self, orderId):
        self.nextOrderId = orderId
        self.connected_flag = True

    # -------------------------------------
    # POSITION UPDATES
    # -------------------------------------
    def position(self, account, contract, pos, avgCost):
        self.position_data[contract.conId] = pos

    # -------------------------------------
    # CONTRACT HELPERS
    # -------------------------------------
    @staticmethod
    def build_future(local_symbol, exchange):
        c = Contract()
        c.secType = "FUT"
        c.exchange = exchange
        c.currency = "USD"
        c.localSymbol = local_symbol
        return c

    # -------------------------------------
    # BASIC MARKET ORDER
    # -------------------------------------
    def submit_market(self, contract, action, qty):
        oid = self.nextOrderId
        self.nextOrderId += 1

        order = Order()
        order.orderType = "MKT"
        order.action = action
        order.totalQuantity = qty

        self.placeOrder(oid, contract, order)
        return oid