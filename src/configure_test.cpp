/*
 * configure_test.cpp
 *
 *  Created on: 2017年3月20日
 *      Author: ShunTan
 */
#include <stdio.h>
#include "zookeeper_helper.h"

class CConfigureZkRead: public zookeeper::CWatcherAction
{
public:
    CConfigureZkRead();
    ~CConfigureZkRead();

public:
    virtual void on_node_created(zhandle_t*, const char* path)
    {
        //printf("on_node_created \n");
    }

    virtual void on_nodevalue_changed(zhandle_t*, const char* path)
    {
        //printf("on_nodevalue_changed\n");
        clear();
        get(_config);
        printf("config changed !\n");
    }

    virtual void on_node_deleted(zhandle_t*, const char* path)
    {
        //printf("on_node_deleted\n");
    }

public:
    int  run();
    bool get(std::string& config);

private:
    zookeeper::CZookeeperHelper* _zk;
    std::string _config;
};

CConfigureZkRead::CConfigureZkRead() : _zk(NULL)
{
}

CConfigureZkRead::~CConfigureZkRead()
{
    delete _zk;
}

int CConfigureZkRead::run()
{
    if(get(_config))
    {
        printf("get config success ! \n");
    }
    else
    {
        printf("get config failed ! \n");
        return 0;
    }

    while(true)
    {
        printf("do work with conf[%s] \n", _config.c_str());
        sleep(1);
    }
}

bool CConfigureZkRead::get(std::string& config)
{
    if(_zk == NULL)
    {
        _zk = new zookeeper::CZookeeperHelper("10.143.130.31:2181");
    }

    if(_zk->connect())
    {
        return _zk->zookeeper_get("/shit", &config, this);
    }
    else
    {
        printf("error:%s\n", _zk->get_last_error().c_str());
    }

    return false;
}

///////////////////////////////////////////////////////////////////////////////////////////

class CConfigureZkWrite
{
public:
    CConfigureZkWrite();
    ~CConfigureZkWrite();

public:
    int  run();
    bool set(const std::string& config);

private:
    zookeeper::CZookeeperHelper* _zk;
};

CConfigureZkWrite::CConfigureZkWrite(): _zk(NULL)
{
}

CConfigureZkWrite::~CConfigureZkWrite()
{
    delete _zk;
}

int CConfigureZkWrite::run()
{
    int i = 0;
    std::string config_suffer = "FUCK_YOU_";

    while(true)
    {
        char str[sizeof("065535")]; // 0xFFFF
        snprintf(str, sizeof(str), "%d", i);

        if(set((config_suffer+str)))
        {
            printf("set config[%s] success!\n", (config_suffer+str).c_str() );
            i++;
        }
        else
        {
            return -1;
        }

        sleep(10);
    }
}

bool CConfigureZkWrite::set(const std::string& config)
{
    if(_zk == NULL)
    {
        _zk = new zookeeper::CZookeeperHelper("10.143.130.31:2181");
    }

    if(_zk->connect())
    {
        if(!_zk->zookeeper_set("/shit", config))
        {
            if(_zk->get_last_errorcode() == ZNONODE)
            {
                std::string new_path;
                if(!_zk->zookeeper_create("/shit", config, &new_path))
                {
                    printf("error:(%s) code[%d]\n", _zk->get_last_error().c_str(), _zk->get_last_errorcode());
                }
                else
                {
                    return true;
                }
            }

        }
        else
            return true;
    }
    else
    {
        printf("error:%s\n", _zk->get_last_error().c_str());
    }

    return false;
}

/////////////////////////////////////////////////////////////////

extern "C" int main(int argc, char* argv[])
{
    if(argc != 2)
    {
        printf("arguments num error!\n");
        return -1;
    }

    if(std::string(argv[1]) == "read")
    {
        CConfigureZkRead zr;
        zr.run();
    }
    else if(std::string(argv[1]) == "write")
    {
        CConfigureZkWrite zw;
        zw.run();
    }
    else
    {
        printf("no right argv!\n");
        return -1;
    }

    return 0;
}

