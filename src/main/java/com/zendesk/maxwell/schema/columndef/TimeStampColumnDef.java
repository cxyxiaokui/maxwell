package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class TimeStampColumnDef extends ColumnDefWithLength {

	public TimeStampColumnDef(String name, String type, short pos, Long columnLength) {
		super(name, type, pos, columnLength);
	}

	static final Logger LOGGER = LoggerFactory.getLogger(TimeStampColumnDef.class);

	final private boolean isTimestamp = getType().equals("timestamp");

	protected String formatValue(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		// special case for those broken mysql dates.
		if (value instanceof Long) {
			Long v = (Long) value;
			if (v == Long.MIN_VALUE || (v == 0L && isTimestamp)) {
				if (config.zeroDatesAsNull)
					return null;
				else
					return appendFractionalSeconds("0000-00-00 00:00:00", 0, columnLength);
			}
		}

		try {
			Timestamp ts = DateFormatter.extractTimestamp(value);
			String dateString = TimeStampFormatter.formatDateTime(value, ts);
			LOGGER.debug("TimeStampColumnDef name:{} formatDateTime:{}", name, dateString);
			return appendFractionalSeconds(dateString, ts.getNanos(), columnLength);
		} catch (IllegalArgumentException e) {
			throw new ColumnDefCastException(this, value);
		}
	}
}
