ahh
# @Time : 2020/9/14 15:11
# @Content : 对es写入操作的重写
from elasticsearch import Elasticsearch, helpers
import datetime
import copy


class NewES:

    def __init__(self):
        self.es = Elasticsearch(hosts="192.168.3.195:9200")
        self.es_index = ''

    def create_index(self, g_index):
        """
        先连接这个es数据库，然后如果索引不存在就新建
        :param g_index: es索引名称
        :return: None
        """
        self.es_index = g_index
        if not self.es.indices.exists(index=g_index):
            self.es.indices.create(index=g_index, ignore=400)

    def index(self, g_index, g_data):
        """
        在索引中创建一个新文档。ID是由es自动生成
        :param g_index:想要增加数据的所在索引
        :param g_data:数据字典，也就是body
        :return:关于结果的字符串
        """
        if not isinstance(g_data, dict):
            return "The parameter must be of a dictionary type"
        elif not self.es.indices.exists(index=g_index):
            return "Index does not exist"
        else:
            self.es.index(index=g_index, body=g_data)
            return 'success'

    def create(self, g_index, g_data, g_id):
        """
        在索引中创建一个新文档。ID是由用户自己提供，所以这种方法并不推荐使用
        :param g_index: 想要增加数据的所在索引
        :param g_data: 数据字典，也就是body
        :param g_id: 用户自己提供的数据id
        :return: 关于结果的字符串
        """
        if not isinstance(g_data, dict):
            return "The parameter must be of a dictionary type"
        elif not self.es.indices.exists(index=g_index):
            return "Index does not exist"
        else:
            self.es.create(index=g_index, body=g_data, id=g_id)
            return 'success'

    def update(self, g_index, g_data, g_id):
        """
        跟新文档某些数据信息
        :param g_index: 更新数据的所在索引
        :param g_data: 数据字典，也就是body
        :param g_id: 更新数据的id
        :return: 关于结果的字符串
        """
        if not isinstance(g_data, dict):
            return "The parameter must be of a dictionary type"
        elif not self.es.indices.exists(index=g_index):
            return "Index does not exist"
        else:
            self.es.update(index=g_index, body=g_data, id=g_id)
            return 'success'

    def bulk(self, args):
        """
        使用生成器批量写入数据
        :param args: 用户传入一个数据列表，内容是一个个的字典
        :return:关于结果的字符串
        """
        if not isinstance(args, list):
            return "The parameter must be of a list type"
        else:
            actions = []
            for line in args:
                action = {
                    "_index": self.es_index,
                    "_source": {
                    }
                }
                if "timestamp" not in line.keys() or line['timestamp']:
                    action["_source"]["timestamp"] = datetime.datetime.now()
                for key in line.keys():
                    action["_source"][key] = line[key]
                actions.append(copy.deepcopy(action))
                action.clear()
                print(actions)
            helpers.bulk(self.es, actions)
            return 'success'

    def close(self):
        """
        关闭传输和连接
        :return: None
        """
        self.transport.close()
