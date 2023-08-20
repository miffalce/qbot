# qbot

## How to start

1. 请复制.env_pub为.env,参数模板如.env_pub所示
2. 请参阅nonebot文档，使用```pip3 install nonebot2```等,暂时先根据No moudle 自行修复，待完善本项目后，再提供requirement文件
3. 本项目新增插件为名为```nonebot_plugin_status```。
    ```store_handler为数据库存储.```
    ```scheduled_job为协程中定时update message_tb_{guild_id}的spam,通过post请求返回spam值.```
    ```recall_message会定时从数据库select出sapm < 阈值的值(-1，1), 越接近1代表着该message信息越正常. <0的值为不健康的值.```
4. 数据库暂有1+n张表
``` user_tb
      Column      |            Type             | Collation | Nullable |               Default                
------------------+-----------------------------+-----------+----------+--------------------------------------
 mid              | integer                     |           | not null | nextval('user_tb_mid_seq'::regclass)
 author_id        | character varying           |           |          | 
 author_username  | character varying           |           |          | 
 author_bot       | boolean                     |           |          | 
 author_avatar    | character varying           |           |          | 
 channel_id       | character varying           |           |          | 
 guild_id         | character varying           |           |          | 
 member_nick      | character varying           |           |          | 
 member_roles     | integer[]                   |           |          | 
 member_joined_at | timestamp without time zone |           |          | 
 spam             | double precision            |           |          | 
 color            | integer                     |           |          | 
Indexes:
    "user_tb_pkey" PRIMARY KEY, btree (mid)
    "ix_user_tb_author_id" btree (author_id)
    "ix_user_tb_channel_id" btree (channel_id)
    "ix_user_tb_guild_id" btree (guild_id)
```

``` message_tb_{guild_id} 代表着有多个频道(子频道共享guild_id)
   Column   |            Type             | Collation | Nullable |                           Default                            
------------+-----------------------------+-----------+----------+--------------------------------------------------------------
 mid        | integer                     |           | not null | nextval('message_tb_14278872714059729096_mid_seq'::regclass)
 id         | character varying           |           |          | 
 channel_id | character varying           |           |          | 
 author_id  | character varying           |           |          | 
 content    | character varying           |           |          | 
 timestamp  | timestamp without time zone |           |          | 
 spam       | double precision            |           |          | 
 color      | integer  
```
5. spam列的的rest api使用bert的二分类暂不开放，请自行训练.
6. 本项目强依赖pg数据库，若不想，只需要在event中直接发起post请求即可。


## Documentation

See [Docs](https://nonebot.dev/)
