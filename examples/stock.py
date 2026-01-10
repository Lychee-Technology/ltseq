
from ltseq.expr.base import if_else
from pathlib import Path

from ltseq import LTSeq

# Task: Find all time intervals where a stock rose for more than 3 consecutive days.

DATA_PATH = Path(__file__).with_name("StockRecords.csv")

t = LTSeq.read_csv(str(DATA_PATH))

result = (
    t
    .sort(lambda r: r.CODE, lambda r: r.DT)
    # 1. Procedural: Calculate rise/fall status relative to previous row
    .derive(lambda r: {
        "is_up": if_else(
                r.CL.shift(1).is_null() | (r.CODE != r.CODE.shift(1)),
                False, 
                r.CL > (r.CL.shift(1)*1.095)
        )
    })
    # 2. Ordered Grouping: Cut a new group only when 'is_up' changes
    #    (Groups consecutive True records together)
    .group_ordered(lambda r: (r.CODE, r.is_up))
    
    # 3. Filtering on Sub-tables (Groups)
    #    'g' represents a sub-table object
    .filter(lambda g: 
        (g.first().is_up) and           # Must be a rising group
        (g.count() >= 3)               # Must last >= 3 days
    )
    # 4. Extract info from the group
    .derive(lambda g: {
        "start": g.first().DT,
        "end":   g.last().DT,
        "gain":  (g.last().CL - g.first().CL) / g.first().CL,
        "days":  g.count()
    })
    .distinct(lambda r: r.CODE)
    .select(lambda r: r.CODE, lambda r: r.start, lambda r:r.end, lambda r:r.days, lambda r:r.gain)
)

result.show()