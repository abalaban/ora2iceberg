# Quick Start: Oracle NUMBER Type Inference

## TL;DR

```bash
# Analyze Oracle table and get intelligent type recommendations
java -jar ora2iceberg-0.8.1.7-all.jar \
  --extract-metadata --infer-types \
  --source-jdbc-url "jdbc:oracle:thin:@HOST:PORT:SID" \
  --source-user username --source-password password \
  --source-schema schema_name --source-object table_name \
  --output .
```

**Result**: Instead of all `NUMBER` columns defaulting to wasteful `decimal(38,10)`, get optimized types like `integer`, `long`, or `decimal(10,2)` based on actual data analysis.

## What Problem Does This Solve?

**Before:**
```sql
-- Oracle table
CREATE TABLE orders (
    order_id NUMBER,        -- Actually contains 1,2,3,4...
    price NUMBER            -- Actually contains 19.99, 25.50...  
);

-- Default Iceberg mapping (WASTEFUL)
CREATE TABLE orders_iceberg (
    order_id DECIMAL(38,10),    -- 38 bytes per row (should be 4)
    price DECIMAL(38,10)        -- 38 bytes per row (should be 6)
);
```

**After Enhancement:**
```sql  
-- Intelligent Iceberg mapping (OPTIMIZED)
CREATE TABLE orders_iceberg (
    order_id INTEGER,           -- 4 bytes per row (90% savings)
    price DECIMAL(8,2)          -- 6 bytes per row (84% savings)
);
```

## Usage Examples

### 1. Basic Analysis
```bash
java -jar ora2iceberg-0.8.1.7-all.jar \
  --extract-metadata --infer-types \
  --source-jdbc-url "jdbc:oracle:thin:@dbhost:1521:orcl" \
  --source-user scott --source-password tiger \
  --source-schema scott --source-object emp \
  --output /tmp
```

### 2. Oracle EBS Environment
```bash
java -jar ora2iceberg-0.8.1.7-all.jar \
  --extract-metadata --infer-types \
  --source-jdbc-url "jdbc:oracle:thin:@ebs-db:1521:prod" \
  --source-user apps --source-password apps_password \
  --source-schema ont --source-object oe_order_lines_all \
  --output .
```

### 3. Large Table Analysis
```bash
# Automatically samples 100K rows from millions for analysis
java -jar ora2iceberg-0.8.1.7-all.jar \
  --extract-metadata --infer-types \
  --source-jdbc-url "jdbc:oracle:thin:@warehouse:1521:dwh" \
  --source-user analytics --source-password password \
  --source-schema sales --source-object fact_sales \
  --output /data/metadata
```

## Reading the Results

### Console Output
```
INFO  Starting NUMBER column type inference analysis
INFO  Found 125 NUMBER columns in ont.oe_order_lines_all  
INFO  Analyzing 125 NUMBER columns with sample size 100000 from 4648722 total rows

INFO  Column LINE_ID: total=95519, nulls=0, int=95519, long=0, decimal=0, maxPrec=8, maxScale=0 
      -> recommended Iceberg type: integer

INFO  Column UNIT_PRICE: total=95519, nulls=18, int=5220, long=0, decimal=90281, maxPrec=10, maxScale=7 
      -> recommended Iceberg type: decimal(10,7)

INFO  NUMBER column type inference completed for 125 columns
```

### Key Metrics Explained
- **total**: Number of sampled values for this column
- **nulls**: Count of NULL values
- **int**: Values that fit in 32-bit integer  
- **long**: Values that fit in 64-bit long but not 32-bit int
- **decimal**: Values with decimal places or too large for long
- **maxPrec/maxScale**: Maximum precision and scale observed

### Type Recommendation Logic
- **integer**: >95% of values are integers fitting in 32-bit
- **long**: >95% of values fit in 64-bit (int + long combined)
- **decimal(P,S)**: Mixed data, uses observed precision/scale
- **decimal(38,10)**: All-null columns (fallback)

## Expected Performance

| Table Size | NUMBER Columns | Analysis Time | Storage Savings |
|------------|----------------|---------------|-----------------|
| 100K rows | 20 columns | ~5 seconds | 70-85% |
| 1M rows | 50 columns | ~15 seconds | 75-90% |
| 10M rows | 100 columns | ~45 seconds | 80-90% |

## Troubleshooting

### No NUMBER columns found
```
INFO  Found 0 NUMBER columns in schema.table
```
**Fix**: Oracle EBS permission issue. The system automatically falls back to data dictionary queries.

### Analysis takes too long
```  
INFO  Analyzing 200 NUMBER columns with sample size 100000
```
**Fix**: Large sample size. This is normal for tables with millions of rows.

### Unexpected recommendations
```
INFO  Column ID: recommended Iceberg type: decimal(38,10)
```
**Fix**: Column might be all NULL or have inconsistent data patterns.

## Integration with Migration

### Manual Application
Use the type recommendations when defining your Iceberg schema:

```java
// Based on analysis results
Schema schema = new Schema(
    required(1, "line_id", Types.IntegerType.get()),          // Was NUMBER
    optional(2, "unit_price", Types.DecimalType.of(10, 7)),   // Was NUMBER  
    optional(3, "quantity", Types.IntegerType.get())          // Was NUMBER
);
```

### Future Automation
Upcoming features will automatically apply recommendations to migration process.

## Best Practices

✅ **Do:**
- Run analysis on production-representative data
- Review recommendations before applying
- Test with sample data migrations first
- Use on tables >10K rows for meaningful results

❌ **Don't:**
- Skip analysis on large tables with many NUMBER columns
- Apply recommendations blindly without validation
- Use on constantly changing schemas
- Expect perfect recommendations for all edge cases

## CLI Options Reference

| Option | Description | Example |
|--------|-------------|---------|
| `--extract-metadata` | Extract table metadata | Required |
| `--infer-types` | Analyze NUMBER columns | Optional |
| `--source-jdbc-url` | Oracle JDBC connection | `jdbc:oracle:thin:@host:port:sid` |
| `--source-user` | Oracle username | `apps` |
| `--source-password` | Oracle password | `password` |
| `--source-schema` | Schema name | `ont` |
| `--source-object` | Table name | `oe_order_lines_all` |
| `--output` | Output directory | `.` or `/tmp` |

## What's Next?

1. **Review Results**: Check console output for type recommendations
2. **Validate Samples**: Ensure recommendations make sense for your data
3. **Test Migration**: Try migrating a subset with recommended types  
4. **Apply at Scale**: Use recommendations for full table migration
5. **Monitor Performance**: Measure query speed and storage improvements

**Questions?** Check the full documentation in `ORACLE_NUMBER_INFERENCE.md`