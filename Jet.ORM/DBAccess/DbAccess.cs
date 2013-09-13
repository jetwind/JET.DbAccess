using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using System.Configuration;

namespace DBAccess
{
    public class DbAccess:IDisposable
    {
        /// <summary>
         /// 安全类型的集合
         /// </summary>
         private static Hashtable parmCache = Hashtable.Synchronized(new Hashtable());
         /// <summary>
         /// 下面两个是静态变量
         /// </summary>
         private static readonly string strs = ConfigurationManager.ConnectionStrings["ConnLink"].ConnectionString;
         private static readonly string pdn = ConfigurationManager.ConnectionStrings["ConnLink"].ProviderName;
 
         /// <summary>
         /// 属性用于接收数据的存取类型 及识别用户所启用的数据库
         /// </summary>
         private string providername;
         /// <summary>
         /// 各种数据库的连接字符串
         /// </summary>
         private string connstring;
 
         #region " 带参和不带参的构造函数 "
         /// <summary>
         /// 默认构造函数，有重载
         /// </summary>
         public DbAccess()
         {
             this.providername = pdn;    //使用的数据驱动类,默认
             this.connstring = strs;     //连接数据库的字符串,默认
         }
 
         /// <summary>
         /// 初始构造函数
         /// </summary>
         /// <param name="provider">数据驱动类型[SqlClient|Access|Orarl|SQLite]</param>
         /// <param name="links">数据库的连接字符串</param>
         public DbAccess(string provider, string links)
         {
             this.providername = provider;   //使用的数据驱动类
             this.connstring = links;        //连接数据库的字符串
         }
         #endregion
 
         /// <summary>
         /// 析构函数
         /// </summary>
         ~DbAccess()
         {
             CloseCon();   //热行清理
         }
 
 
         /// <summary>
         /// 检测当前数据库连接状态
         /// </summary>
         /// <returns></returns>
         public string isConnstate()
         {
             if (cmd != null)
             {
                 return cmd.Connection.State.ToString();
             }
             return "变量已清除";
         }
 
         #region "isclose属性,默认0指进行清理，1为不进行清理，可以用于多次循环之中，避免多次开关数据库"
         /// <summary>
         /// 是否进行各项数据库连接器的清理工作
         /// </summary>
         /// <returns></returns>
         private int isclose = 0;
         public int IsClose
         {
             get
             {
                 return isclose;
             }
             set
             {
                 isclose = value;
             }
         }
         #endregion
 
         #region " 分页记录反回变量 "
         private int allpage = 0;      ///分页函数中记录共有多少页的变量
         public int Allpage
         {
             get { return allpage; }
         }
 
         private int allrecord = 0;    ///分页函数中记录菜有数据总量的变量
         public int Allrecord
         {
             get { return allrecord; }
         }
         #endregion
 
         #region " 数据库操作对像的属性[Adapter|Command|Begintransaction] "
         /// <summary>
         /// 属性DbDataAdapter
         /// </summary>
         private DbDataAdapter adp;
 
         /// <summary>
         /// 属性SqlCommand
         /// </summary>
         private DbCommand cmd;
 
         /// <summary>
         /// 事务
         /// </summary>
         private DbTransaction Tran;
         #endregion
 
         #region " CloseCon() 关闭相关的数据库连接 "
         /// <summary>
         /// 关闭数据库连接
         /// </summary>
         public void CloseCon()
         {
             if (cmd != null)
             {
                 if (cmd.Connection.State != ConnectionState.Closed)
                 {
                     cmd.Connection.Close();
                 }
                 cmd.Dispose();
                 cmd = null;
             }
 
             if (adp != null)
             {
                 adp.Dispose();
                 adp = null;
             }
 
             if (Tran != null)
             {
                 Tran.Dispose();
                 Tran = null;
             }
 
             //GC.Collect();       ///强制对所有代进行垃圾回收
         }
         #endregion
         
         #region "getFace() 创建工厂对像"
         /// <summary>
         /// 创建工厂对像
         /// </summary>
         /// <returns>DbProviderFactory</returns>
         public DbProviderFactory GetFace()
         {
             DbProviderFactory fact = null;
             //if (providername == "System.Data.SQLite")
             //{
             //    fact = SQLiteFactory.Instance;     //SQLite数据库创建数据工厂类
             //}
             //else
             //{
                 fact = DbProviderFactories.GetFactory(providername);  //获得当前所调定的数据源存取类型
             //}
             return fact;
         }
         #endregion
 
         #region " 创建CMD对像，以供其它对像使用 "
         /// <summary>
         /// 创建CMD对像
         /// </summary>
         /// <returns>DbComand对像实例</returns>
         private DbCommand CreateDbCommand()
         {
             DbProviderFactory fact = GetFace();           //工厂对像
             DbConnection conn = fact.CreateConnection();  //创建Connection对像
             conn.ConnectionString = connstring;           //设定Connection对像的连接字符串
             cmd = conn.CreateCommand();                   //使用 conn 的函数 CreateCommand() 创建Command对像
             return cmd;                                   //返回Command对像
         }
         #endregion
 
         #region " RemoveParames清除CMD的参数 及存储过程的参数缓存 "
         /// <summary>
         /// 清除参数
         /// </summary>
         /// <param name="cmd">DbCommand的对像</param>
         private void RemoveParams(DbCommand cmd)
         {
             while (cmd.Parameters.Count > 0)
             {
                 cmd.Parameters.RemoveAt(0);
             }
         }
 
         /// <summary>
         /// 从缓存中初使化SQL或存储过程的参数
         /// </summary>
         /// <param name="cmd">Command对像</param>
         /// <returns>布尔值</returns>
         public bool initParametersFromCache(DbCommand cmd)
         {
             DbParameter[] parms = GetCachedParameters(string.Format("{0}{1}", cmd.Connection.ConnectionString, cmd.CommandText));
             if (parms == null)
                 return false;
             for (int i = 0; i < parms.Length; i++)
             {
                 cmd.Parameters.Add(parms[i]);
             }
             return true;
         }
 
         public static void CacheParameters(string cacheKey, params DbParameter[] cmdParms)
         {
             parmCache[cacheKey] = cmdParms;
         }
 
         /// <summary>
         /// 查找缓存中的变量
         /// </summary>
         /// <param name="cacheKey">缓存名称</param>
         /// <returns>DbParameter</returns>
         public DbParameter[] GetCachedParameters(string cacheKey)
         {
             DbParameter[] cachedParms = (DbParameter[])parmCache[cacheKey];  //parmCache 本程序第18行，安全类型的HASHtable
 
             if (cachedParms == null)
                 return null;
 
             DbParameter[] clonedParms = new DbParameter[cachedParms.Length];
 
             for (int i = 0, j = cachedParms.Length; i < j; i++)
             {
                 clonedParms[i] = (DbParameter)((ICloneable)cachedParms[i]).Clone();
             }
 
             return clonedParms;
         }
 
         /// <summary>
         /// 缓存参数
         /// </summary>
         /// <param name="cmd">DbCommand</param>
         public void CachedParameters(DbCommand cmd)
         {
             DbParameterCollection paramColl = cmd.Parameters;
             DbParameter[] parms = new DbParameter[paramColl.Count];
             for (int i = 0; i < paramColl.Count; i++)
             {
                 parms[i] = paramColl[i];
             }
 
             CacheParameters(string.Format("{0}{1}", cmd.Connection.ConnectionString, cmd.CommandText), parms);
         }
 
 
         #endregion
 
         #region " ProTxtCmd创建操作存储过程与SQL的CMD对像 "
         /// <summary>
         /// 创建操作存储过程与SQL的CMD对像
         /// </summary>
         /// <param name="pronames">存储过程或SQL语句</param>
         /// <param name="sid">识别ID</param>
         /// <returns>cmd</returns>
         private DbCommand ProTxtCmd(string proSqls, Byte sid)
         {
             cmd = CreateDbCommand();                                               //创建CMD对像
             if (sid == 0)
             {
                 cmd.CommandType = CommandType.Text;                                //设置SQL语句
             }
             else
             {
                 cmd.CommandType = CommandType.StoredProcedure;                     //设置 cmd 的操作命令方式，此处为存储过程
             }
 
             cmd.CommandText = proSqls;                                            //设置 存储过程名或SQL语句
             return cmd;
         }
         #endregion
 
         #region "executeRunSqlArray  执行多条SQL语句，有事务，批量删除等，传递一个数组"
         /// <summary>
         /// 执行多条SQL语句，有事务
         /// </summary>
         /// <param name="sqllist">SQL语句集合数组</param>
         /// <returns>Boolean</returns>
         public bool executeRunSqlArray(string[] sqllist)
         {
             cmd = CreateDbCommand();                                               //创建CMD对像
             try
             {
                 if (cmd.Connection.State != ConnectionState.Open)
                 {
                     cmd.Connection.Open();
                 }
                 Tran = cmd.Connection.BeginTransaction();               //新增事务
                 cmd.Transaction = Tran;                       //事务
                 for (int i = 0; i < sqllist.Length; i++)
                 {
                     string sql = sqllist[i].ToString();
                     if (sql.Trim().Length > 1)
                     {
                         cmd.CommandText = sql;
                         cmd.ExecuteNonQuery();
                     }
                 }
                 Tran.Commit();
             }
             catch (DbException ex)
             {
                 Tran.Rollback();
                 printEx(ex, 1);
                 return false;
             }
             finally
             {
                 if (this.isclose == 0)   //默认清理
                 {
                     CloseCon();   //热行清理
                 }
             }
 
             return true;
 
         }
         #endregion
 
         #region " getdt 从SQL语句或存储过程中返回DataTable "
         /// <summary>
         /// 返回SQLS中的dt
         /// </summary>
         /// <param name="sqlsPro">SQL语句</param>
         /// <param name="parames">SQL语句参数</param>
         /// <param name="sid">识别码，0为SQL语句，1为存储过程</param>
         /// <returns>DateTable</returns>
         public DataTable getdt(string sqlsPro, DbParameter[] parames, int sid)
         {
             DataTable dt = new DataTable();
             DbDataReader dr = null;
 
             if (sid == 0)     //SQL语句
             {
                 cmd = ProTxtCmd(sqlsPro, 0);
             }
             else              //存储过程
             {
                 cmd = ProTxtCmd(sqlsPro, 1);
             }
 
             if (parames != null && parames.Length > 0)
             {
                 foreach (DbParameter param in parames)
                     if (param.Value != null)
                         cmd.Parameters.Add(param);
             }
 
             try
             {
                 if (cmd.Connection.State != ConnectionState.Open)
                 {
                     cmd.Connection.Open();
                 }
                 dr = cmd.ExecuteReader(CommandBehavior.CloseConnection); 
                 if (dr.HasRows)
                 {
                     dt.Load(dr);
                     dr.Close();
                     dr = null;
                 }
             }
             catch (DbException ex)
             {
                 dr.Close();
                 dr = null;
                 printEx(ex, 1);
             }
             finally
             {
                 if (this.isclose == 0)   //默认清理
                 {
                     if (parames != null)
                     {
                         RemoveParams(cmd);
                         cmd.Parameters.Clear();
                     }
                     CloseCon();   //热行清理
                 }
             }
             return dt;
         }
         #endregion
 
         #region " runsql 执行SQL语句 "
         /// <summary>
         /// 运行SQL语句
         /// </summary>
         /// <param name="sqlsPro">sql语句</param>
         /// <param name="parames">SQL语句参数</param>
         /// <param name="sid">识别ID</param>
         /// <returns>int</returns>
         public int runsql(string sqlsPro, DbParameter[] parames, int sid)
         {
             int rsInt = 0;
 
             cmd = null;
 
             if (sid == 0)     //SQL语句
             {
                 cmd = ProTxtCmd(sqlsPro, 0);
             }
             else              //存储过程
             {
                 cmd = ProTxtCmd(sqlsPro, 1);
             }
 
             if (parames != null && parames.Length > 0)
             {
                 foreach (DbParameter param in parames)
                     if (param.Value != null)
                         cmd.Parameters.Add(param);
             }
 
             try
             {
                 if (cmd.Connection.State != ConnectionState.Open)
                 {
                     cmd.Connection.Open();
                 }
 
                 rsInt = cmd.ExecuteNonQuery();
             }
             catch (DbException ex)
             {
                 printEx(ex, 1);
             }
             finally
             {
                 if (this.isclose == 0)   //默认清理
                 {
                     if (parames != null)
                     {
                         RemoveParams(cmd);
                         cmd.Parameters.Clear();
                     }
                     CloseCon();   //热行清理
                 }
             }
 
             return rsInt;
 
         }
         #endregion
 
         #region " getSca 泛型，取得单个数据ExecuteScalar，适用于SQL语句及存储过程 "
         /// <summary>
         /// 取得单个数据  (注意如果取整型数据请使用long代替T)
         /// </summary>
         /// <typeparam name="T">泛型所替换的数据类型</typeparam>
         /// <param name="sqlsPro">sql语句</param>
         /// <param name="parames">SQL语句的参数</param>
         /// <param name="sid">识别ID，0为SQL语句，1为存储过程</param>
         /// <returns></returns>
         public T getSca<T>(string sqlsPro, DbParameter[] parames, int sid)
         {
             cmd = null;
 
             if (sid == 0)     //SQL语句
             {
                 cmd = ProTxtCmd(sqlsPro, 0);
             }
             else              //存储过程
             {
                 cmd = ProTxtCmd(sqlsPro, 1);
             }
 
             if (parames != null && parames.Length > 0)
             {
                 foreach (DbParameter param in parames)
                     if (param.Value != null)
                         cmd.Parameters.Add(param);
             }
             T Tstr = default(T);   ///泛型变量
             try
             {
                 if (cmd.Connection.State != ConnectionState.Open)
                 {
                     cmd.Connection.Open();
                 }
 
                 if (System.DBNull.Value != cmd.ExecuteScalar())
                 {
                     Tstr = (T)cmd.ExecuteScalar();           ///读取第一行第一列
                 }
 
             }
             catch (DbException ex)
             {
                 printEx(ex, 0);
                 return default(T);
             }
             finally
             {
                 if (this.isclose == 0)   //默认清理
                 {
                     if (parames != null)
                     {
                         RemoveParams(cmd);
                         cmd.Parameters.Clear();
                     }
                     CloseCon();   //热行清理
                 }
             }
 
             return Tstr;
 
         }
         #endregion
 
         #region " 使用Adapter填充DataTable "
         /// <summary>
         /// 返回SQLS中的dt
         /// </summary>
         /// <param name="sqlsPro">SQL语句</param>
         /// <param name="parames">SQL语句参数</param>
         /// <param name="sid">识别码，0为SQL语句，1为存储过程</param>
         /// <returns>DateTable</returns>
         public DataTable getAdpdt(string sqlsPro, DbParameter[] parames, Byte sid)
         {
             DbProviderFactory dbfactory = GetFace();  //取得数据库工厂对像
 
             adp = dbfactory.CreateDataAdapter();
 
             if (sid == 0)     //SQL语句
             {
                 cmd = ProTxtCmd(sqlsPro, 0);
             }
             else              //存储过程
             {
                 cmd = ProTxtCmd(sqlsPro, 1);
             }
 
             if (parames != null && parames.Length > 0)
             {
                 foreach (DbParameter param in parames)
                     if (param.Value != null)
                         cmd.Parameters.Add(param);
             }
 
             adp.SelectCommand = cmd;
             DataTable dt = new DataTable();
             try
             {
                 adp.Fill(dt);
             }
             catch (DbException ex)
             {
                 printEx(ex, 1);
             }
             finally
             {
                 if (this.isclose == 0)   //默认清理
                 {
                     if (parames != null)
                     {
                         RemoveParams(cmd);
                         cmd.Parameters.Clear();
                     }
 
                     CloseCon();   //热行清理
                 }
             }
             return dt;
         }
         #endregion
 
         #region " Pagination_dt 分页,适用于SQL SERVER数据库 "
         /// <summary>
         /// 分页函数，返回dt
         /// </summary>
         /// <param name="table_name">需要分页显示的表名</param>
         /// <param name="key">表的主键,必须唯一性</param>
         /// <param name="orderstr">排序字段如f_Name asc或f_name desc(注意只能有一个排序字段)</param>
         /// <param name="cpage">当前页</param>
         /// <param name="psize">每页大小</param>
         /// <param name="fieles">显示的字段列表</param>
         /// <param name="filter">条件语句,不加where</param>
         /// <param name="g_str">分组字段</param>     以前都不再使用
         /// <param name="pro_name">存储过程名</param>
         /// <returns>返回内存表</returns>
         public DataTable Pagination_dt(string table_name, string key, string orderstr, int cpage, int psize, string fieles, string filter, string g_str, string pro_name)
         {
 
             DataTable dt = new DataTable();
 
             SqlCommand cmd = (SqlCommand)ProTxtCmd(pro_name, 1);
 
             SqlParameter[] parames = { new SqlParameter("@Tables", SqlDbType.VarChar, 50), new SqlParameter("@PrimaryKey", SqlDbType.VarChar, 10), new SqlParameter("@Sort", SqlDbType.VarChar, 50), new SqlParameter("@CurrentPage", SqlDbType.Int, 4), new SqlParameter("@PageSize", SqlDbType.Int, 4), new SqlParameter("@fields", SqlDbType.VarChar, 1000), new SqlParameter("@Filter", SqlDbType.VarChar, 1000), new SqlParameter("@Group", SqlDbType.VarChar, 1000) };
 
             parames[0].Value = table_name;
             parames[1].Value = key;
             parames[2].Value = orderstr;
             parames[3].Value = cpage;
             parames[4].Value = psize;
             parames[5].Value = fieles;
             parames[6].Value = filter;
             parames[7].Value = g_str;
 
             foreach (SqlParameter parameter in parames)  ///添加输入参数集合
             {
                 cmd.Parameters.Add(parameter);
             }
 
             SqlParameter s1 = cmd.Parameters.Add(new SqlParameter("@TotalPage", SqlDbType.Int));
             SqlParameter s2 = cmd.Parameters.Add(new SqlParameter("@TotalRecord", SqlDbType.Int));    ///返回值
             s1.Direction = ParameterDirection.Output;
             s2.Direction = ParameterDirection.Output;
 
             DbDataReader dr = null;
 
             try
             {
                 if (cmd.Connection.State != ConnectionState.Open)
                 {
                     cmd.Connection.Open();
                 }
 
                 dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);                               //关闭数据库
                 if (dr.HasRows)
                 {
                     dt.Load(dr);
                     dr.Close();
                     dr.Dispose();
 
                     if (cmd.Parameters["@TotalRecord"].Value != System.DBNull.Value)
                     {
                         allrecord = int.Parse(cmd.Parameters["@TotalRecord"].Value.ToString()); ///返回总记录数
                     }
 
 
                     if (cmd.Parameters["@TotalPage"].Value != System.DBNull.Value)
                     {
                         allpage = int.Parse(cmd.Parameters["@TotalPage"].Value.ToString());  ///返回首页数
                     }
                 }
             }
             catch (DbException ex)
             {
                 dr.Close();
                 dr = null;
                 printEx(ex, 1);
             }
             finally
             {
                 if (parames != null)
                 {
                     RemoveParams(cmd);
                     cmd.Parameters.Clear();
                 }
                 CloseCon();       //热行清理
             }
 
             return dt;
         }
         #endregion
 
         #region " Access_dt 分页，适用于ACCESS及其其它使用SQL语句的分页 "
         /// <summary>
         /// SQL语句分页函数，返回dt
         /// </summary>
         /// <param name="table_name">需要分页显示的表名</param>
         /// <param name="key">表的主键ID,且只能为ID必须唯一性</param>
         /// <param name="orderstr">排序字段如id ASC,addTimes DESC或id DESC,addTimes ASC(可以有多个排序字段)</param>
         /// <param name="cpage">当前页</param>
         /// <param name="psize">每页大小</param>
         /// <param name="fieles">显示的字段列表</param>
         /// <param name="filter">条件语句,不加where</param>
         /// <param name="isDesc">排序方式[true=desc倒序|false=asc顺序]</param>
         /// <param name="allrecordsqls">客户端传来的计算总记录的SQL语句</param>
         /// <param name="sql">客户端发来的分页SQL语句</param>
         /// <returns>返回内存表</returns>
         public DataTable Access_dt(string table_name, string key, string orderstr, int cpage, int psize, string fieles, string filter, bool isDesc, string allrecordsqls, string sql)
         {
             //先计算总记录，再计算总页数
             string allCordSql = null;
             if (filter == null)   //不存在WHERE子句
             {
                 allCordSql = "SELECT COUNT(" + key + ") FROM " + table_name + "";
             }
             else
             {
                 allCordSql = "SELECT COUNT(" + key + ") FROM " + table_name + " WHERE " + filter + "";
             }
 
             if (allrecordsqls == null)
             {
                 allrecord = getSca<int>(allCordSql, null, 0);                             ///返回总记录数
             }
             else
             {
                 allrecord = getSca<int>(allrecordsqls, null, 0);                          ///返回总记录数
             }
 
             if (allrecord % psize == 0)
             {
                 allpage = allrecord / psize;        //返回总页数
             }
             else
             {
                 allpage = allrecord / psize + 1;   //返回总页数
             }
 
             string sqls = null;
 
             if (isDesc)   //倒序[从大到小]
             {
                 if (filter == null)   //不存在WHERE子句
                 {
                     if (cpage == 1)
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " ORDER BY " + orderstr + "";   //第一页
                     }
                     else
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " WHERE " + key + "<(SELECT MIN(" + key + ") FROM (SELECT TOP " + (cpage - 1) * psize + " " + key + " FROM " + table_name + " ORDER BY " + orderstr + ") as T) ORDER BY " + orderstr + "";   //非第一页
                     }
                 }
                 else
                 {
                     if (cpage == 1)
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " WHERE " + filter + " ORDER BY " + orderstr + "";   //第一页
                     }
                     else
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " WHERE " + filter + " AND " + key + "<(SELECT MIN(" + key + ") FROM (SELECT TOP " + (cpage - 1) * psize + " " + key + " FROM " + table_name + " WHERE " + filter + " ORDER BY " + orderstr + ") as T) ORDER BY " + orderstr + "";   //非第一页
                     }
                 }
             }
             else       //顺序[从小到大]
             {
                 if (filter == null)   //不存在WHERE子句
                 {
                     if (cpage == 1)
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " ORDER BY " + orderstr + "";   //第一页
                     }
                     else
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " WHERE " + key + ">(SELECT MAX(" + key + ") FROM (SELECT TOP " + (cpage - 1) * psize + " " + key + " FROM " + table_name + " ORDER BY " + orderstr + ") as T) ORDER BY " + orderstr + "";   //非第一页
                     }
                 }
                 else
                 {
                     if (cpage == 1)
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " WHERE " + filter + " ORDER BY " + orderstr + "";   //第一页
                     }
                     else
                     {
                         sqls = "SELECT TOP " + psize + " " + fieles + " FROM " + table_name + " WHERE " + filter + " AND " + key + ">(SELECT MAX(" + key + ") FROM (SELECT TOP " + (cpage - 1) * psize + " " + key + " FROM " + table_name + " WHERE " + filter + " ORDER BY " + orderstr + ") as T) ORDER BY " + orderstr + "";   //非第一页
                     }
                 }
             }
             //到此构造分页语句完成
 
             DataTable dt = new DataTable();
 
             if (sql == null)     //客户端没有传入SQL语句
             {
                 dt = this.getdt(sqls.ToString(), null, 0);
             }
             else                //有传入
             {
                 dt = this.getdt(sql, null, 0);
             }
 
             return dt;
 
         }
         #endregion
 
         #region " SQLite分页,只适用于SQLite数据库"
         /// <param name="tablename">需要分页显示的表名</param>
         /// <param name="key">表的主键ID,且只能为ID必须唯一性</param>
         /// <param name="orderstr">排序字段如id ASC,addTimes DESC或id DESC,addTimes ASC(可以有多个排序字段)</param>
         /// <param name="cpage">当前页</param>
         /// <param name="psize">每页大小</param>
         /// <param name="fieles">显示的字段列表</param>
         /// <param name="filter">条件语句,不加where</param>
         /// <param name="allrecordsqls">客户端传来的计算总记录的SQL语句</param>
         /// <param name="sql">客户端发来的分页SQL语句</param>
         /// <returns>返回内存表</returns>
         public DataTable SQLite_dt(string tablename, string key, string orderstr, int cpage, int psize, string fieles, string filter, string allrecordsqls, string sql)
        {
            //先计算总记录，再计算总页数
            string allCordSql = null;
            if (filter == null)   //不存在WHERE子句
            {
                allCordSql = "SELECT COUNT([" + key + "]) FROM [" + tablename + "]";
            }
            else
            {
                allCordSql = "SELECT COUNT([" + key + "]) FROM [" + tablename + "] WHERE " + filter + "";
            }

            if (allrecordsqls == null)
            {
                allrecord = int.Parse(this.getSca<long>(allCordSql, null, 0).ToString());                             ///返回总记录数
            }
            else
            {
                allrecord = int.Parse(this.getSca<long>(allrecordsqls, null, 0).ToString());                          ///返回总记录数
            }

            if (allrecord % psize == 0)
            {
                allpage = allrecord / psize;        //返回总页数
            }
            else
            {
                allpage = allrecord / psize + 1;   //返回总页数
            }

            StringBuilder sqls = new StringBuilder("SELECT " + fieles + " FROM [" + tablename + "]");

            if (filter != null)   //不存在WHERE子句
            {
                sqls.Append(" WHERE " + filter + " ");   //非第一页
            }

            if (cpage == 1)
            {
                sqls.Append(" ORDER BY " + orderstr + " LIMIT " + psize);   //第一页
            }
            else
            {
                sqls.Append(" ORDER BY " + orderstr + " LIMIT " + (psize * cpage - psize) + "," + psize + "");   //非第一页
            }

            //到此构造分页语句完成     throw new Exception("出现异常：" + sqls.ToString());

            DataTable dt = new DataTable();

            if (sql == null)     //客户端没有传入SQL语句
            {
                dt = this.getdt(sqls.ToString(), null, 0);
            }
            else                //有传入
            {
                dt = this.getdt(sql, null, 0);
            }

            return dt;
        }
        #endregion

        #region " IDisposable 成员 "
        /// <summary>
        /// 强迫释放数据库连接
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// 类回收时，关闭数据库连接
        /// </summary>
        /// <param name="disposing"></param>
        public void Dispose(bool disposing)
        {
            if (disposing)
            {
                CloseCon();
            }
            else
            {
                CloseCon();
            }
        }

        #endregion

        #region " showpage 显示分页数 "
        /// <summary>
        /// url调整
        /// </summary>
        /// <param name="str">网址</param>
        /// <returns>string</returns>
        public string joinchar(string str)
        {
            if (str == "")
            {
                return "";
            }

            if (str.IndexOf("?") < str.Length)
            {
                if (str.IndexOf("?") > 1)
                {
                    if (str.IndexOf("&") < str.Length)
                    {
                        return str + "&";
                    }
                    else
                    {
                        return str;
                    }
                }
                else
                {
                    return str + "?";
                }
            }

            return str;
        }

        /// <summary>
        /// 显示分页数
        /// </summary>
        /// <param name="total">记录数</param>
        /// <param name="pagenum">每页个数</param>
        /// <param name="current">当前页</param>
        /// <param name="url">页面url</param>
        /// <param name="unit">单位[条|个|位]</param>
        /// <returns>string</returns>
        public string showpage(int total, int pagenum, int current, string url, string unit)
        {
            StringBuilder str = new StringBuilder();
            StringBuilder str1 = new StringBuilder();

            int page = 1;
            if (total % pagenum == 0)
            {
                page = total / pagenum;
            }
            else
            {
                page = total / pagenum + 1;
            }
            url = joinchar(url);

            if (page > 10)
            {
                if (current <= 5)
                {
                    for (int i = 1; i <= 9; i++)
                    {
                        if (i == current)
                        {
                            str.Append(" <b>[" + i + "]</b> ");
                        }
                        else
                        {
                            str.Append(" <a href='" + url + "page=" + i + "'>" + i + "</a> ");
                        }
                    }
                    str.Append("" + " <a href='" + url + "page=" + page + "'>" + page + "</a> ");
                }
                else if (current >= page - 4)
                {
                    str.Append(" <a href='" + url + "page=1'>1</a> ");
                    for (int i = page - 8; i <= page; i++)
                    {
                        if (i == current)
                        {
                            str.Append(" <b>[" + i + "]</b> ");
                        }
                        else
                        {
                            str.Append(" <a href='" + url + "page=" + i + "'>" + i + "</a> ");
                        }
                    }
                }
                else
                {
                    str.Append(" <a href='" + url + "page=1'>1</a> ");
                    for (int i = current - 4; i <= current + 4; i++)
                    {
                        if (i == current)
                        {
                            str.Append(" <b>[" + i + "]</b> ");
                        }
                        else
                        {
                            str.Append(" <a href='" + url + "page=" + i + "'>" + i + "</a> ");
                        }
                    }
                    str.Append("" + " <a href='" + url + "page=" + page + "'>" + page + "</a> ");
                }
            }
            else
            {
                for (int i = 1; i <= page; i++)
                {
                    if (page != 1)
                    {
                        if (i == current)
                        {
                            str.Append(" <b>[" + i + "]</b> ");
                        }
                        else
                        {
                            str.Append(" <a href='" + url + "page=" + i + "'>" + i + "</a> ");
                        }
                    }
                }
            }
            int down = current + 1;
            int up = current - 1;
            if (page > 1)
            {
                if (current == 1)
                {
                    str1.Append(" <a href='" + url + "page=2'>下一页</a>");
                }
                else if (current == page)
                {
                    str1.Append(" <a href='" + url + "page=" + up.ToString() + "'>上一页</a>");
                }
                else
                {
                    str1.Append(" <a href='" + url + "page=" + up.ToString() + "'>上一页</a> <a href='" + url + "page=" + down.ToString() + "'>下一页</a>");
                }
            }
            if (page == 0) { page = 1; }

            return "页次:" + pagenum + "/" + current + "/" + page.ToString() + ",共<span id='rs_count'>" + total.ToString() + "</span>" + unit + " &nbsp; " + str1.ToString() + str.ToString();
        }
        #endregion

        #region "printEx 打印异常"
        /// <summary>
        /// 打印异常
        /// </summary>
        /// <param name="ex">异常集合</param>
        /// <param name="id">是否显示错误</param>
        private void printEx(DbException e, int id)
        {
            if (this.isclose == 1)   //如果isclose的值是1说明没有执行清理工作，所以在异常收集函数里面执行清理工作
            {
                CloseCon();         //热行清理
            }
            if (id == 1)
            {
                string errstr = "错误如下：<br />" + "出错信息：" + e.Message + "<br />" + "出错来源：" + e.Source + "<br />" + "程序：" + e.ErrorCode + "<br />异常方法：" + e.TargetSite;
                throw new Exception("出现异常：" + errstr);
            }
        }
        #endregion
    }
}
