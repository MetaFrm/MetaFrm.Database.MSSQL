using System.Data.Common;
using System.Data.SqlClient;

namespace MetaFrm.Database
{
    /// <summary>
    /// MSSQL Database 클래스 입니다.
    /// </summary>
    public class MSSQL : IDatabase
    {
        readonly SqlDataAdapter sqlDataAdapter;

        /// <summary>
        /// MSSQL Database 클래스 생성자 입니다.
        /// </summary>
        public MSSQL()
        {
            this.sqlDataAdapter = new SqlDataAdapter
            {
                SelectCommand = new SqlCommand
                {
                    Connection = new SqlConnection()
                }
            };
        }

        DbParameter IDatabase.AddParameter(string parameterName, DbType dbType, int size)
        {
            SqlCommand sqlCommand;

            sqlCommand = this.sqlDataAdapter.SelectCommand;

            if (size == 0)
                return sqlCommand.Parameters.Add(parameterName, DbTypeConvert(dbType));
            else
                return sqlCommand.Parameters.Add(parameterName, DbTypeConvert(dbType), size);
        }

        static System.Data.SqlDbType DbTypeConvert(DbType dbType)
        {
            return dbType switch
            {
                DbType.BigInt => System.Data.SqlDbType.BigInt,
                DbType.Binary => System.Data.SqlDbType.Binary,
                DbType.Bit => System.Data.SqlDbType.Bit,
                DbType.Char => System.Data.SqlDbType.Char,
                DbType.Date => System.Data.SqlDbType.Date,
                DbType.DateTime => System.Data.SqlDbType.DateTime,
                DbType.DateTime2 => System.Data.SqlDbType.DateTime2,
                DbType.DateTimeOffset => System.Data.SqlDbType.DateTimeOffset,
                DbType.Decimal => System.Data.SqlDbType.Decimal,
                DbType.Float => System.Data.SqlDbType.Float,
                DbType.Image => System.Data.SqlDbType.Image,
                DbType.Int => System.Data.SqlDbType.Int,
                DbType.Money => System.Data.SqlDbType.Money,
                DbType.NChar => System.Data.SqlDbType.NChar,
                DbType.NText => System.Data.SqlDbType.NText,
                DbType.NVarChar => System.Data.SqlDbType.NVarChar,
                DbType.Real => System.Data.SqlDbType.Real,
                DbType.SmallDateTime => System.Data.SqlDbType.SmallDateTime,
                DbType.SmallInt => System.Data.SqlDbType.SmallInt,
                DbType.SmallMoney => System.Data.SqlDbType.SmallMoney,
                DbType.Structured => System.Data.SqlDbType.Structured,
                DbType.Text => System.Data.SqlDbType.Text,
                DbType.Time => System.Data.SqlDbType.Time,
                DbType.Timestamp => System.Data.SqlDbType.Timestamp,
                DbType.TinyInt => System.Data.SqlDbType.TinyInt,
                DbType.Udt => System.Data.SqlDbType.Udt,
                DbType.UniqueIdentifier => System.Data.SqlDbType.UniqueIdentifier,
                DbType.VarBinary => System.Data.SqlDbType.VarBinary,
                DbType.VarChar => System.Data.SqlDbType.VarChar,
                DbType.Variant => System.Data.SqlDbType.Variant,
                DbType.Xml => System.Data.SqlDbType.Xml,
                _ => System.Data.SqlDbType.Variant,
            };
        }

        DbCommand IDatabase.Command
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand;
            }
        }

        DbConnection IDatabase.Connection
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Connection;
            }
        }

        DbDataAdapter IDatabase.DataAdapter
        {
            get
            {
                return this.sqlDataAdapter;
            }
        }

        DbTransaction IDatabase.Transaction
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Transaction;
            }
        }

        void IDatabase.DeriveParameters()
        {
            SqlCommandBuilder.DeriveParameters(this.sqlDataAdapter.SelectCommand);
        }

        void IDatabase.Close()
        {
            if (this.sqlDataAdapter.SelectCommand.Connection != null)
            {
                this.sqlDataAdapter.SelectCommand.Connection.Close();
                this.sqlDataAdapter.SelectCommand.Connection.Dispose();
            }

            if (this.sqlDataAdapter.SelectCommand != null)
            {
                this.sqlDataAdapter.SelectCommand.Dispose();
            }

            if (this.sqlDataAdapter != null)
            {
                this.sqlDataAdapter.Dispose();
            }
        }
    }
}