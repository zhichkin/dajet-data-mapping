using System;
using System.Collections.Generic;

namespace DaJet.Json
{
    public interface IDaJetJsonSerializer
    {
        IEnumerable<ReadOnlyMemory<byte>> Serialize(int pageSize, int pageNumber);
    }
}