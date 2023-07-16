# unicum
Parsing, store and show  sales info from vending autos 
General schema diagram
```mermaid
  flowchart TD;
      A-[Scheduled tasks]->B{Time to parse ? };
      B -- Yes -->C[Parsing to SQLite DB];
      B -- No -->D[Sleep];
      C---->E[Waiting for next task];
      D---->E[Waiting for next task];
```
