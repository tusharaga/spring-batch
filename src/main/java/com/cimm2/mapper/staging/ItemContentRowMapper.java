package com.cimm2.mapper.staging;

import com.tushar.cimm2.db.model.tables.records.ExportStagingItemRecord;
import com.tushar.cimm2.db.model.tables.records.TmbBrandRecord;
import com.tushar.cimm2.db.model.tables.records.TmbManufacturerRecord;
import org.apache.commons.lang.Validate;
import org.jooq.types.ULong;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class ItemContentRowMapper implements RowMapper<ExportStagingItemRecord> {

    Map<ULong, TmbBrandRecord> brandRecordMap;
    Map<ULong, TmbManufacturerRecord> manufacturerRecordMap;
    int i=1;

    public ItemContentRowMapper( Map<ULong, TmbManufacturerRecord> manufacturerRecordMap,Map<ULong, TmbBrandRecord> brandRecordMap){
        this.manufacturerRecordMap = manufacturerRecordMap;
        this.brandRecordMap = brandRecordMap;
    }

    @Override
    public ExportStagingItemRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
        System.out.println("ItemContentRowMapper  :" + i++ + " - Thread name:" + Thread.currentThread().getName() + " Object hashcode:" + this);
        ExportStagingItemRecord record = new ExportStagingItemRecord();
        record.setClientId(rs.getLong("CLIENT_ID"));
        record.setPartNumber(rs.getString("PART_NUMBER"));
        setBrandAndManufactur(record, rs);
        record.setManufacturerPartNumber(rs.getString("MFR_PART_NUMBER"));
        record.setUpc(rs.getString("UPC"));
        record.setVolumeUom(rs.getString("VOLUME_UOM"));
        record.setCountry(rs.getString("COUNTRY_OF_ORIGIN"));
        record.setShortDesc(rs.getString("SHORT_DESCRIPTION"));
        record.setLongDesc1(rs.getString("LONG_DESCRIPTION"));
        record.setMarketingDescription(rs.getString("MRKT_DESCRIPTION"));
        record.setItemFeatures_1(rs.getString("FEATURE_BULLETS"));
        record.setInvoiceDesc(rs.getString("INVC_DESCRIPTION"));
        record.setClientName(rs.getLong("CLIENT_ID")+"");
        return record;
    }

    private void setBrandAndManufactur(ExportStagingItemRecord record, ResultSet rs) throws SQLException {
        String brandId = rs.getString("BRAND_ID");
        Validate.notNull(brandId, "BRAND_ID can't be null");

        TmbBrandRecord tmbBrandRecord = brandRecordMap.get(ULong.valueOf(brandId));
        Validate.notNull(tmbBrandRecord, "Could not find Brand record for BRAND_ID=" + brandId);

        TmbManufacturerRecord tmbManufacturerRecord = manufacturerRecordMap.get(tmbBrandRecord.getManufacturerId());
        Validate.notNull(tmbBrandRecord, "Could not find Manufacturer record for BRAND_ID=" + brandId);

        record.setBrandId(tmbBrandRecord.getBrandCode());
        record.setBrandName(tmbBrandRecord.getBrandName());
        record.setManufacturerId(tmbManufacturerRecord.getManufacturerCode());
        record.setManufacturerName(tmbManufacturerRecord.getManufacturerName());
    }

}
