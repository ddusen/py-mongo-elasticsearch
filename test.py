import os
import datetime
from utils.format import datetime_to_string


def test_open():
    with open('mapping.json', 'r') as file:
        print(file.read())
    

def test_format_time():
    data = {'store': 'DC274867-D323-4DA6-85FA-CB8240286B55', 'store_id': '3850145953759625217', 'store_name': '盐城商业大厦店', 'store_code': '10000', 'store_type': 'DRS', 'store_us_id': 42686, 'operator': '', 'operator_name': '', 'upload': None, 'hash': '09XX63EJI6F739MB14L50NTEH16435699', 'action_type': 'SYNC', 'terminal_open_time': datetime.datetime(2017, 5, 31, 13, 16, 12), 'sales_date': datetime.datetime(2017, 5, 31, 0, 0), 'created': datetime.datetime(2017, 5, 31, 8, 44, 7), 'payload': {'transaction_type': 'order', 'body': {'pre_sell_amount': 0, 'hex_no_cup_item_quantity': 2, 'hex_no_cup_net_amount': 4, 'hex_no_cup_amount': 4, 'hex_amount': 25, 'hex_trans_discount_amount': 0, 'hex_dis_amount': 0, 'hex_discount_count': 0, 'hex_master_discount_amount': 0, 'hex_discount_list': [], 'hex_has_discount': {'master': False, 'item_coupon': False, 'order_coupon': False, 'item': False, 'order': False}, 'coupons_order_dis_amount': 0, 'coupons_dis_amount': 0, 'es_amount': 0, 'hex_discount_id': 0, 'hex_trans_payments': [], 'is_cancel': False, 'open_user_id': 35, 'disc_user_id': 0, 'pay_comment': '', 'pay_channel': '', 'item_amount': 25, 'payments': [{'org_pay_amount': 25, 'hex_category_name': '异业支付', 'hex_category_id': '3835290105308774414', 'hex_trans_discounts': [], 'hex_trans_discount_amount': 0, 'hex_priority': 3, 'hex_pay_amount': 25, 'hex_id': '3850456906871078913', 'tips_amount': 0, 'rule_amount': 0, 'tender_name': '支付宝支付', 'db_fav_data': 0, 'n_link_discount_id': 0, 'dis_amount': 0, 'amount': 25, 'id': 11, 'guid': '6BC985A0-7631-4606-959F-557105CF3AA1', 'dis_rate': 0, 'order_no': 7516549, 'pay_time': '2017-05-31 13:17:16', 'es_amount': 0, 'tender_id': 9910066, 'db_fav_amount': 0, 'tender_type': 9, 'bus_date': '2017-05-31 00:00:00', 'change': 0, 'pay_amount': 25}, {'org_pay_amount': 25, 'hex_category_name': '异业支付', 'hex_category_id': '3835290105308774414', 'hex_trans_discounts': [], 'hex_trans_discount_amount': 0, 'hex_priority': 3, 'hex_pay_amount': 25, 'hex_id': '3850456906871078913', 'tips_amount': 0, 'rule_amount': 0, 'tender_name': '支付宝支付', 'db_fav_data': 0, 'n_link_discount_id': 0, 'dis_amount': 0, 'amount': 25, 'id': '2222', 'guid': '111', 'dis_rate': 0, 'order_no': 7516549, 'pay_time': '2017-05-31 13:17:16', 'es_amount': 0, 'tender_id': 9910066, 'db_fav_amount': 0, 'tender_type': 9, 'bus_date': '2017-05-31 00:00:00', 'change': 0, 'pay_amount': 25}], 'xgu_id': '951AA6BF-06E6-4607-9EBD-C74E9C0AA747', 'chargehostid': 0, 'peoples': 1, 'address': '', 'items': [{'hex_dis_amount': 0, 'dis_amount': 0, 'hex_net_amount': 21, 'net_amount': 21, 'coupons_item_dis_amount': 0, 'hex_discount_id': 0, 'hex_trans_payments': [], 'hex_extends': {'spec_1': 'need update'}, 'unit_id': '3932395049817853953', 'hex_is_cup': True, 'hex_main_type': 'NORMAL', 'hex_id': '3855949758258479105', 'categories': [{'parent_id': '3895644930529886115', 'code': '1402', 'name': '小杯暴风雪', 'id': '3895644930529886152'}, {'parent_id': '3895644930529886101', 'code': '08', 'name': '暴风雪系列', 'id': '3895644930529886115'}, {'code': '00', 'name': 'DQ产品', 'id': '3895644930529886101'}], 'category': {'parent_id': '3895644930529886115', 'code': '1402', 'name': '小杯暴风雪', 'id': '3895644930529886152'}, 'hex_item_discount_amount': 0, 'hex_item_trans_payment_amount': 0, 'hex_order_discount_balance_with_trans': 0, 'hex_order_discount_balance': 0, 'hex_trans_payment_balance': 0, 'hex_trans_discount_balance': 0, 'catalog_id': 74, 'meal_deal_master_id': '', 'item_id': 10010, 'dis_odr_amount': 0, 'dis_itm_amount': 0, 'price': 21, 'unit': '9oz', 'guid': '951AA6BF-06E6-4607-9EBD-C74E9C0AA747', 'discount': 0, 'qty': 1, 'dis_style': 0, 'credit_total': 0, 'dis_name': '', 'amount_ex': 21, 'depart_id': 6, 'credit_amount': 0, 'parent_id': 0, 'plu': '', 'n_discount_grp': 7, 'dis_key_id': 0, 'name': '小杯 奇脆巧克力', 'coupons': [], 'amount': 21, 'key_id': '30', 'order_no': 7516549, 'type': 'normal', 'bus_date': '2017-05-31 00:00:00', 'base_price': 21, 'meal_deal_item_id': ''}, {'hex_dis_amount': 0, 'dis_amount': 0, 'hex_net_amount': 2, 'net_amount': 2, 'coupons_item_dis_amount': 0, 'hex_discount_id': 0, 'hex_trans_payments': [], 'hex_extends': {'spec_1': 'need update'}, 'unit_id': '3932395049817853953', 'hex_is_cup': False, 'hex_main_type': 'NORMAL', 'hex_id': '3850201444674174977', 'categories': [{'parent_id': '3895644930529886118', 'code': '1701', 'name': '标准加料', 'id': '3895644930529886163'}, {'parent_id': '3895644930529886101', 'code': '17', 'name': '加料', 'id': '3895644930529886118'}, {'code': '00', 'name': 'DQ产品', 'id': '3895644930529886101'}], 'category': {'parent_id': '3895644930529886118', 'code': '1701', 'name': '标准加料', 'id': '3895644930529886163'}, 'hex_item_discount_amount': 0, 'hex_item_trans_payment_amount': 0, 'hex_order_discount_balance_with_trans': 0, 'hex_order_discount_balance': 0, 'hex_trans_payment_balance': 0, 'hex_trans_discount_balance': 0, 'catalog_id': 111, 'meal_deal_master_id': '', 'item_id': 50008, 'dis_odr_amount': 0, 'dis_itm_amount': 0, 'price': 2, 'unit': '份', 'guid': '951AA6BF-06E6-4607-9EBD-C74E9C0AA747', 'discount': 0, 'qty': 1, 'dis_style': 0, 'credit_total': 0, 'dis_name': '', 'amount_ex': 2, 'depart_id': 6, 'credit_amount': 0, 'parent_id': 0, 'plu': '1040', 'n_discount_grp': 7, 'dis_key_id': 0, 'name': '巧克力块', 'coupons': [], 'amount': 2, 'key_id': '31', 'order_no': 7516549, 'type': 'normal', 'bus_date': '2017-05-31 00:00:00', 'base_price': 2, 'meal_deal_item_id': ''}, {'hex_dis_amount': 0, 'dis_amount': 0, 'hex_net_amount': 2, 'net_amount': 2, 'coupons_item_dis_amount': 0, 'hex_discount_id': 0, 'hex_trans_payments': [], 'hex_extends': {'spec_1': 'need update'}, 'unit_id': '3932395049817853953', 'hex_is_cup': False, 'hex_main_type': 'NORMAL', 'hex_id': '3850201444120526849', 'categories': [{'parent_id': '3895644930529886118', 'code': '1701', 'name': '标准加料', 'id': '3895644930529886163'}, {'parent_id': '3895644930529886101', 'code': '17', 'name': '加料', 'id': '3895644930529886118'}, {'code': '00', 'name': 'DQ产品', 'id': '3895644930529886101'}], 'category': {'parent_id': '3895644930529886118', 'code': '1701', 'name': '标准加料', 'id': '3895644930529886163'}, 'hex_item_discount_amount': 0, 'hex_item_trans_payment_amount': 0, 'hex_order_discount_balance_with_trans': 0, 'hex_order_discount_balance': 0, 'hex_trans_payment_balance': 0, 'hex_trans_discount_balance': 0, 'catalog_id': 111, 'meal_deal_master_id': '', 'item_id': 50007, 'dis_odr_amount': 0, 'dis_itm_amount': 0, 'price': 2, 'unit': '份', 'guid': '951AA6BF-06E6-4607-9EBD-C74E9C0AA747', 'discount': 0, 'qty': 1, 'dis_style': 0, 'credit_total': 0, 'dis_name': '', 'amount_ex': 2, 'depart_id': 6, 'credit_amount': 0, 'parent_id': 0, 'plu': '1054', 'n_discount_grp': 7, 'dis_key_id': 0, 'name': '巴旦木', 'coupons': [], 'amount': 2, 'key_id': '32', 'order_no': 7516549, 'type': 'normal', 'bus_date': '2017-05-31 00:00:00', 'base_price': 2, 'meal_deal_item_id': ''}], 'pay_method': '', 'pay_benefit': 0, 'dis_amount': 0, 'none_bus_amount': 0, 'disc_name': '', 'yuni_number': '24AS133BK350DA9E', 'discount': 0, 'present_amount': 0, 'card_no': '', 'open_user_name': '关雪珍', 'disc_style': 0, 'customer_name': '', 'close_time': '2017-05-31 13:17:15', 'pos_name': 'SP-800', 'close_user_name': '关雪珍', 'pay_amount': 0, 'item_count': 3, 'close_user_id': 35, 'disc_key_id': 0, 'sum_change': 0, 'coupons': [], 'amount': 25, 'open_time': '2017-05-31 13:16:12', 'order_no': 7532270, 'pos_id': 1, 'disc_item_amount': 0, 'disc_order_amount': 0, 'bus_date': '2017-05-31 00:00:00', 'active_code': '24AS133BK350DA9E', 'phone': '', 'net_amount': 25}, 'header': {'from': datetime.datetime(2016, 7, 4, 6, 15, 18), 'to': datetime.datetime(2016, 7, 4, 6, 15, 18), 'total': 9}}, 'store_branch': [{'parent_id': '3957727696825438209', 'name': '唐嫣', 'id': '3850141429833662465'}, {'parent_id': '3850141420400672769', 'name': '华北一区 邢廷义', 'id': '3957727696825438209'}, {'name': '全市场', 'id': '3850141420400672769'}], 'store_geo': [{'parent_id': '3850141871149940737', 'name': '盐城', 'id': '3850141871653257217'}, {'parent_id': '3850141870009090049', 'name': '江苏省', 'id': '3850141871149940737'}, {'name': '中国', 'id': '3850141870009090049'}], '__v': 0}
    print(data)
    print("-"*100)
    datetime_to_string(data)
    print(data)


def test_oplog():
    from time import sleep

    from pymongo import MongoClient, ASCENDING
    from pymongo.cursor import CursorType
    from pymongo.errors import AutoReconnect

    # Time to wait for data or connection.
    _SLEEP = 1

    config = {
            'host': '106.14.94.38',
            'port': 27071,
            'db': 'saas_dq_uat',
            'table': 'pos'
        }
    oplog = MongoClient(
            config['host'],
            config['port'],
        ).local.oplog.rs
    stamp = oplog.find().sort('$natural', ASCENDING).limit(-1).next()['ts']

    while True:
        kw = {}

        kw['filter'] = {'ts': {'$gt': stamp}}
        kw['cursor_type'] = CursorType.TAILABLE_AWAIT
        kw['oplog_replay'] = True

        cursor = oplog.find(**kw)

        try:
            while cursor.alive:
                for doc in cursor:
                    stamp = doc['ts']

                    print(doc)  # Do something with doc.

                sleep(_SLEEP)

        except AutoReconnect:
            sleep(_SLEEP)


def main():
    test_oplog()


if __name__ == '__main__':
    main()