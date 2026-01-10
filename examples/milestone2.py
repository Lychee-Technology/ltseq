from ltseq import LTSeq

# The lambda is compiled to a Logical Plan, not executed by Python
t = LTSeq.read_csv("users.csv")
t.filter(lambda r: r.age > 18).select(lambda r: [r.name]).show()