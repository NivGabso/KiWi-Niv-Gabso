# KiWi
KiWi improvement - Niv Gabso & Assaf Yifrach. 
We take a relatively fresh wait-free, concurrent sorted map called KiWi, fix and enhance it. First, we test its linearizability by fuzzing and applying Wing&amp;Gong [2] linearizability test. After fixing a few bugs in the algorithm design and its implementation, we enhance it. We design, implement and test two new linearizable operations sizeLowerBound() and sizeUpperBound(). We further compose these operations to create more useful operations. Last, we evaluate the map performance because previous evaluations became obsolete due to our bug corrections.

