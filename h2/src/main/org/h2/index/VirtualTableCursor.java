/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * A cursor for a virtual table. This implementation filters the rows (only
 * returns entries that are larger or equal to "first", and smaller than last or
 * equal to "last").
 */
class VirtualTableCursor implements Cursor {

    private final VirtualTableIndex index;

    private final SearchRow first;

    private final SearchRow last;

    private final ResultInterface result;

    Value[] values;

    Row row;

    /**
     * @param index
     *            index
     * @param first
     *            first row
     * @param last
     *            last row
     * @param result
     *            the result
     */
    VirtualTableCursor(VirtualTableIndex index, SearchRow first, SearchRow last,
            ResultInterface result) {
        this.index = index;
        this.first = first;
        this.last = last;
        this.result = result;
    }

    @Override
    public Row get() {
        if (values == null) {
            return null;
        }
        if (row == null) {
            row = Row.get(values, 1);
        }
        return row;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        final SearchRow first = this.first, last = this.last;
        if (first == null && last == null) {
            return nextImpl();
        }
        while (nextImpl()) {
            Row current = get();
            if (first != null) {
                int comp = index.compareRows(current, first);
                if (comp < 0) {
                    continue;
                }
            }
            if (last != null) {
                int comp = index.compareRows(current, last);
                if (comp > 0) {
                    continue;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Skip to the next row if one is available. This method does not filter.
     *
     * @return true if another row is available
     */
    private boolean nextImpl() {
        row = null;
        if (result != null && result.next()) {
            values = result.currentRow();
        } else {
            values = null;
        }
        return values != null;
    }

    @Override
    public boolean previous() {
        throw DbException.getInternalError(toString());
    }

}
