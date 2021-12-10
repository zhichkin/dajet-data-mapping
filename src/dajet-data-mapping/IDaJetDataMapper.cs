using DaJet.Data.Mapping;
using System.Collections.Generic;
using System.Data;

namespace DaJet.Data
{
    public interface IDaJetDataMapper
    {
        void Reconfigure();
        DataMapperOptions Options { get; }
        void Configure(DataMapperOptions options);
        int GetTotalRowCount();
        long TestGetPageDataRows(int size, int page);
        IEnumerable<IDataReader> GetPageDataRows(int size, int page);
    }
}