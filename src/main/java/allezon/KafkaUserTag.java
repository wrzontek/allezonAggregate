/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package allezon;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;

@org.apache.avro.specific.AvroGenerated
public class KafkaUserTag extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 1677824470275493269L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"KafkaUserTag\",\"namespace\":\"allezon\",\"fields\":[{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"origin\",\"type\":\"string\"},{\"name\":\"brand_id\",\"type\":\"string\"},{\"name\":\"category_id\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<KafkaUserTag> ENCODER =
      new BinaryMessageEncoder<KafkaUserTag>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<KafkaUserTag> DECODER =
      new BinaryMessageDecoder<KafkaUserTag>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<KafkaUserTag> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<KafkaUserTag> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<KafkaUserTag> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<KafkaUserTag>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this KafkaUserTag to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a KafkaUserTag from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a KafkaUserTag instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static KafkaUserTag fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public CharSequence time;
  @Deprecated public CharSequence origin;
  @Deprecated public CharSequence brand_id;
  @Deprecated public CharSequence category_id;
  @Deprecated public int price;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public KafkaUserTag() {}

  /**
   * All-args constructor.
   * @param time The new value for time
   * @param origin The new value for origin
   * @param brand_id The new value for brand_id
   * @param category_id The new value for category_id
   * @param price The new value for price
   */
  public KafkaUserTag(CharSequence time, CharSequence origin, CharSequence brand_id, CharSequence category_id, Integer price) {
    this.time = time;
    this.origin = origin;
    this.brand_id = brand_id;
    this.category_id = category_id;
    this.price = price;
  }

  public SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return time;
    case 1: return origin;
    case 2: return brand_id;
    case 3: return category_id;
    case 4: return price;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: time = (CharSequence)value$; break;
    case 1: origin = (CharSequence)value$; break;
    case 2: brand_id = (CharSequence)value$; break;
    case 3: category_id = (CharSequence)value$; break;
    case 4: price = (Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'time' field.
   * @return The value of the 'time' field.
   */
  public CharSequence getTime() {
    return time;
  }


  /**
   * Sets the value of the 'time' field.
   * @param value the value to set.
   */
  public void setTime(CharSequence value) {
    this.time = value;
  }

  /**
   * Gets the value of the 'origin' field.
   * @return The value of the 'origin' field.
   */
  public CharSequence getOrigin() {
    return origin;
  }


  /**
   * Sets the value of the 'origin' field.
   * @param value the value to set.
   */
  public void setOrigin(CharSequence value) {
    this.origin = value;
  }

  /**
   * Gets the value of the 'brand_id' field.
   * @return The value of the 'brand_id' field.
   */
  public CharSequence getBrandId() {
    return brand_id;
  }


  /**
   * Sets the value of the 'brand_id' field.
   * @param value the value to set.
   */
  public void setBrandId(CharSequence value) {
    this.brand_id = value;
  }

  /**
   * Gets the value of the 'category_id' field.
   * @return The value of the 'category_id' field.
   */
  public CharSequence getCategoryId() {
    return category_id;
  }


  /**
   * Sets the value of the 'category_id' field.
   * @param value the value to set.
   */
  public void setCategoryId(CharSequence value) {
    this.category_id = value;
  }

  /**
   * Gets the value of the 'price' field.
   * @return The value of the 'price' field.
   */
  public int getPrice() {
    return price;
  }


  /**
   * Sets the value of the 'price' field.
   * @param value the value to set.
   */
  public void setPrice(int value) {
    this.price = value;
  }

  /**
   * Creates a new KafkaUserTag RecordBuilder.
   * @return A new KafkaUserTag RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new KafkaUserTag RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new KafkaUserTag RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new KafkaUserTag RecordBuilder by copying an existing KafkaUserTag instance.
   * @param other The existing instance to copy.
   * @return A new KafkaUserTag RecordBuilder
   */
  public static Builder newBuilder(KafkaUserTag other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for KafkaUserTag instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<KafkaUserTag>
    implements org.apache.avro.data.RecordBuilder<KafkaUserTag> {

    private CharSequence time;
    private CharSequence origin;
    private CharSequence brand_id;
    private CharSequence category_id;
    private int price;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.time)) {
        this.time = data().deepCopy(fields()[0].schema(), other.time);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.origin)) {
        this.origin = data().deepCopy(fields()[1].schema(), other.origin);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.brand_id)) {
        this.brand_id = data().deepCopy(fields()[2].schema(), other.brand_id);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.category_id)) {
        this.category_id = data().deepCopy(fields()[3].schema(), other.category_id);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.price)) {
        this.price = data().deepCopy(fields()[4].schema(), other.price);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
    }

    /**
     * Creates a Builder by copying an existing KafkaUserTag instance
     * @param other The existing instance to copy.
     */
    private Builder(KafkaUserTag other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.time)) {
        this.time = data().deepCopy(fields()[0].schema(), other.time);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.origin)) {
        this.origin = data().deepCopy(fields()[1].schema(), other.origin);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.brand_id)) {
        this.brand_id = data().deepCopy(fields()[2].schema(), other.brand_id);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.category_id)) {
        this.category_id = data().deepCopy(fields()[3].schema(), other.category_id);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.price)) {
        this.price = data().deepCopy(fields()[4].schema(), other.price);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'time' field.
      * @return The value.
      */
    public CharSequence getTime() {
      return time;
    }


    /**
      * Sets the value of the 'time' field.
      * @param value The value of 'time'.
      * @return This builder.
      */
    public Builder setTime(CharSequence value) {
      validate(fields()[0], value);
      this.time = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'time' field has been set.
      * @return True if the 'time' field has been set, false otherwise.
      */
    public boolean hasTime() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'time' field.
      * @return This builder.
      */
    public Builder clearTime() {
      time = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'origin' field.
      * @return The value.
      */
    public CharSequence getOrigin() {
      return origin;
    }


    /**
      * Sets the value of the 'origin' field.
      * @param value The value of 'origin'.
      * @return This builder.
      */
    public Builder setOrigin(CharSequence value) {
      validate(fields()[1], value);
      this.origin = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'origin' field has been set.
      * @return True if the 'origin' field has been set, false otherwise.
      */
    public boolean hasOrigin() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'origin' field.
      * @return This builder.
      */
    public Builder clearOrigin() {
      origin = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'brand_id' field.
      * @return The value.
      */
    public CharSequence getBrandId() {
      return brand_id;
    }


    /**
      * Sets the value of the 'brand_id' field.
      * @param value The value of 'brand_id'.
      * @return This builder.
      */
    public Builder setBrandId(CharSequence value) {
      validate(fields()[2], value);
      this.brand_id = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'brand_id' field has been set.
      * @return True if the 'brand_id' field has been set, false otherwise.
      */
    public boolean hasBrandId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'brand_id' field.
      * @return This builder.
      */
    public Builder clearBrandId() {
      brand_id = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'category_id' field.
      * @return The value.
      */
    public CharSequence getCategoryId() {
      return category_id;
    }


    /**
      * Sets the value of the 'category_id' field.
      * @param value The value of 'category_id'.
      * @return This builder.
      */
    public Builder setCategoryId(CharSequence value) {
      validate(fields()[3], value);
      this.category_id = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'category_id' field has been set.
      * @return True if the 'category_id' field has been set, false otherwise.
      */
    public boolean hasCategoryId() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'category_id' field.
      * @return This builder.
      */
    public Builder clearCategoryId() {
      category_id = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'price' field.
      * @return The value.
      */
    public int getPrice() {
      return price;
    }


    /**
      * Sets the value of the 'price' field.
      * @param value The value of 'price'.
      * @return This builder.
      */
    public Builder setPrice(int value) {
      validate(fields()[4], value);
      this.price = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'price' field has been set.
      * @return True if the 'price' field has been set, false otherwise.
      */
    public boolean hasPrice() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'price' field.
      * @return This builder.
      */
    public Builder clearPrice() {
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public KafkaUserTag build() {
      try {
        KafkaUserTag record = new KafkaUserTag();
        record.time = fieldSetFlags()[0] ? this.time : (CharSequence) defaultValue(fields()[0]);
        record.origin = fieldSetFlags()[1] ? this.origin : (CharSequence) defaultValue(fields()[1]);
        record.brand_id = fieldSetFlags()[2] ? this.brand_id : (CharSequence) defaultValue(fields()[2]);
        record.category_id = fieldSetFlags()[3] ? this.category_id : (CharSequence) defaultValue(fields()[3]);
        record.price = fieldSetFlags()[4] ? this.price : (Integer) defaultValue(fields()[4]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<KafkaUserTag>
    WRITER$ = (org.apache.avro.io.DatumWriter<KafkaUserTag>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<KafkaUserTag>
    READER$ = (org.apache.avro.io.DatumReader<KafkaUserTag>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.time);

    out.writeString(this.origin);

    out.writeString(this.brand_id);

    out.writeString(this.category_id);

    out.writeInt(this.price);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.time = in.readString(this.time instanceof Utf8 ? (Utf8)this.time : null);

      this.origin = in.readString(this.origin instanceof Utf8 ? (Utf8)this.origin : null);

      this.brand_id = in.readString(this.brand_id instanceof Utf8 ? (Utf8)this.brand_id : null);

      this.category_id = in.readString(this.category_id instanceof Utf8 ? (Utf8)this.category_id : null);

      this.price = in.readInt();

    } else {
      for (int i = 0; i < 5; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.time = in.readString(this.time instanceof Utf8 ? (Utf8)this.time : null);
          break;

        case 1:
          this.origin = in.readString(this.origin instanceof Utf8 ? (Utf8)this.origin : null);
          break;

        case 2:
          this.brand_id = in.readString(this.brand_id instanceof Utf8 ? (Utf8)this.brand_id : null);
          break;

        case 3:
          this.category_id = in.readString(this.category_id instanceof Utf8 ? (Utf8)this.category_id : null);
          break;

        case 4:
          this.price = in.readInt();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










