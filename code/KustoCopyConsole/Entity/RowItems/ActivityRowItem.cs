using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class ActivityRowItem : RowItemBase
    {
        public ActivityState State { get; set; }

        public string ActivityName { get; set; } = string.Empty;

        public DatabaseIdentity SourceDatabase { get; set; } = DatabaseIdentity.Empty;

        public TableIdentity DestinationTable { get; set; } = TableIdentity.Empty;

        public override void Validate()
        {
            if (string.IsNullOrWhiteSpace(ActivityName))
            {
                throw new InvalidDataException($"{nameof(ActivityName)} must have a value");
            }
            SourceDatabase.Validate();
            DestinationTable.Validate();
        }

        public ActivityRowItem ChangeState(ActivityState newState)
        {
            var clone = (ActivityRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}