package com.pivotal.pxf.filtering;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import com.pivotal.pxf.filtering.FilterParser.ColumnIndex;
import com.pivotal.pxf.filtering.FilterParser.Constant;
import com.pivotal.pxf.filtering.FilterParser.Operation;
import com.pivotal.pxf.utilities.ColumnDescriptor;
import com.pivotal.pxf.utilities.InputData;

public class AccumuloFilterEval implements FilterParser.IFilterEvaluator {
	private PrintWriter wrtr = null;
	private List<ColumnDescriptor> columns = null;

	public AccumuloFilterEval(List<ColumnDescriptor> columns)
			throws FileNotFoundException {
		this.columns = columns;

		wrtr = new PrintWriter(new File("/tmp/acc-file-filter-"
				+ System.currentTimeMillis() + ".log"));

	}

	public AccumuloFilterEval(InputData meta) throws FileNotFoundException {
		this.columns = new ArrayList<ColumnDescriptor>();
		for (int i = 0; i < meta.columns(); ++i) {
			columns.add(meta.getColumn(i));
		}

		wrtr = new PrintWriter(new File("/tmp/acc-file-filter-"
				+ System.currentTimeMillis() + ".log"));

	}

	@SuppressWarnings("unchecked")
	public List<Range> getRanges(String filterString) throws Exception {

		wrtr.println("Filter String: " + filterString);
		wrtr.flush();

		FilterParser parser = new FilterParser(this);
		Object result = parser.parse(filterString);

		if (result instanceof List) {
			return (List<Range>) result;
		} else {
			throw new Exception("String " + filterString
					+ " resolved to no filter");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object evaluate(FilterParser.Operation opId, Object leftOperand,
			Object rightOperand) throws Exception {

		wrtr.println("opId:\t" + opId + "\tLeft:\t" + leftOperand
				+ "\tRight:\t" + rightOperand);
		wrtr.flush();

		List<Range> ranges = null;
		if (leftOperand instanceof ColumnIndex
				&& rightOperand instanceof Constant) {
			ranges = new ArrayList<Range>();

			columnConstantRange(ranges, opId,
					columns.get(((ColumnIndex) leftOperand).index()),
					(Constant) rightOperand);

		} else if (rightOperand instanceof ColumnIndex
				&& leftOperand instanceof Constant) {
			ranges = new ArrayList<Range>();
			constantColumnRange(ranges, opId,
					columns.get(((ColumnIndex) rightOperand).index()),
					(Constant) leftOperand);
		} else if (leftOperand instanceof List
				&& opId == FilterParser.Operation.HDOP_AND
				&& rightOperand instanceof List) {
			ranges = handleCompoundOperations((List<Range>) leftOperand,
					(List<Range>) rightOperand);
		} else {
			wrtr.println("Unhandled filter statement");
			wrtr.flush();
			throw new Exception("Unhandable filter statement");
		}

		return ranges;
	}

	private List<Range> handleCompoundOperations(List<Range> leftOperand,
			List<Range> rightOperand) {

		wrtr.println("handleCompoundOperations called");
		wrtr.flush();

		leftOperand.addAll(rightOperand);
		return leftOperand;
	}

	private void constantColumnRange(List<Range> ranges, Operation opId,
			ColumnDescriptor desc, Constant constant) {

		if (desc.columnName().equals("recordkey")) {
			Range r = null;

			switch (opId) {
			case HDOP_GE:
				r = new Range(null, true, new Text(constant.constant()
						.toString()), true);
				break;
			case HDOP_GT:
				r = new Range(null, true, new Text(constant.constant()
						.toString()), false);
				break;
			case HDOP_LE:
				r = new Range(new Text(constant.constant().toString()), true,
						null, true);
				break;
			case HDOP_LT:
				r = new Range(new Text(constant.constant().toString()), false,
						null, true);
				break;
			default:
				break;
			}

			if (r != null) {
				wrtr.println("addConstantColumnRange added Range: " + r);
				wrtr.flush();
				ranges.add(r);
			} else {
				wrtr.println("addConstantColumnRange did not add a range for "
						+ desc.columnName());
				wrtr.flush();
			}
		}
	}

	private void columnConstantRange(List<Range> ranges, Operation opId,
			ColumnDescriptor desc, Constant constant) throws IOException {

		if (desc.columnName().equals("recordkey")) {
			Range r = null;

			switch (opId) {
			case HDOP_GE:
				r = new Range(new Text(constant.constant().toString()), true,
						null, true);
				break;
			case HDOP_GT:
				r = new Range(new Text(constant.constant().toString()), false,
						null, true);
				break;
			case HDOP_LE:
				r = new Range(null, true, new Text(constant.constant()
						.toString()), true);
				break;
			case HDOP_LT:
				r = new Range(null, true, new Text(constant.constant()
						.toString()), false);
				break;
			default:
				break;
			}

			if (r != null) {
				wrtr.println("addColumnConstantRange added Range: " + r);
				wrtr.flush();
				ranges.add(r);
			} else {
				wrtr.println("addColumnConstantRange did not add a range for "
						+ desc.columnName());
				wrtr.flush();
			}
		}
	}
}
