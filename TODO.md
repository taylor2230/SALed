# TODO
___

CSV:
> * CSV
>  * FromCsv Source
>    * CsvOptions
>    * CSV Input from string
>    * CSV Input from file
>    * Apply schema
>      * Create TableSet with applied schema
>      * Match Fields with input
>      * Read input with schema and create tuples
>      * Needs headers
>    * No Schema
>      * Has headers
>        * Read headers, apply as headers schema
>        * Create tuples and generate schema
>      * No headers
>        * Default columns names based on column count
>        * Create tuples and generate schema
>  * ToCsv Sink
>    * CsvOptions 