/*
 * client_test.cpp
 *
 *  Created on: 2017年2月28日, Tencent Inc.
 *      Author: ShunTan
 */

#include "zookeeper_helper.h"

class CExistsWatcher: public zookeeper::CWatcherAction {
public:
    CExistsWatcher(zookeeper::CZookeeperHelper* zookeeper_helper) :
            m_is_master(false)
    {
        m_zookeeper_helper = zookeeper_helper;

    }

public:
    virtual void on_node_deleted(zhandle_t*, const char* path)
    {
        std::string value_path;
        m_zookeeper_helper->zookeeper_create(path, "shit", &value_path);
        printf("Now, Im' the master \n");
        m_is_master = true;
    }

private:
    bool m_is_master;
    zookeeper::CZookeeperHelper* m_zookeeper_helper;
};

/*
 * 本例子为了测试基于zookeeper的主备切换功能,本机同时开启俩个程式即可看到效果。
 * zookeeper还可应用于负载均衡,配置同步。等各种场景。
 */
extern "C" int main() {
    zookeeper::CZookeeperHelper zk("127.0.0.1:2181");
    if (zk.connect())
    {
        std::string ip;
        uint16_t port;

        if (zk.get_zookeeper_host(&ip, &port))
            printf("client:(%lld)(%s,%d) connect to zookeeper success!\n",
                    zk.get_zookeeper_clientid(), ip.c_str(), port);
    }
    else
    {
        printf("error:%s\n", zk.get_last_error().c_str());
    }

    // TEST MULTI
    std::vector<zookeeper::STOption_t> ops;
    std::vector<zookeeper::STResult_t> results;
    ops.push_back(zk.zookeeper_create_op_init("/ShunTan", "shit"));

    if (!zk.zookeeper_multi(ops, results))
    {
        if (zk.get_last_errorcode() == ZNODEEXISTS)
        {
            printf("node[%s] exist, Im' the cluster. \n", "/ShunTan");
        }
    }
    else
    {
        printf("create node[%s] success, Im' the master. \n",
                results[0].value.c_str());
    }

    /*
     std::string value_path;
     if(!zk.zookeeper_create("/ShunTan", "shit", &value_path))
     {
     //printf("error:%s\n", zk.get_last_error().c_str());
     if(zk.get_last_errorcode() ==  ZNODEEXISTS)
     {
     printf("node[%s] exist, Im' the cluster. \n", "/ShunTan");
     }
     }
     else
     {
     printf("create node[%s] success, Im' the master. \n", value_path.c_str());
     }
     */

    CExistsWatcher exist_watcher(&zk);
    zookeeper::CAsyncCompletion async_completion;
    if (!zk.zookeeper_exists("/ShunTan", &async_completion, &exist_watcher))
    {
        printf("error:%s\n", zk.get_last_error().c_str());
    }
    else
    {
        while (!async_completion.is_triggered())
        {
            usleep(100);
        }
        //printf("async respond success! \n");
    }

    while (!exist_watcher.is_triggered())
    {
        usleep(100);
    }

    return 0;
}


