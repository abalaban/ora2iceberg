/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.oracle.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.junit.jupiter.api.Test;

import java.sql.Types;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class Ora2IcebergTypeMapperTest {
	@Test
	public void testNumber2Integer() {
		Ora2IcebergTypeMapper mapper = new Ora2IcebergTypeMapper(
				null, "%_ID:NUMBER=LONG; %ATED_BY:NUMBER=INTEGER");

				Pair<Integer, Type> lastUpdatedBy = mapper.icebergType("CREATED_BY", java.sql.Types.NUMERIC, 0, 0);
				Type lastUpdatedByType = lastUpdatedBy.getRight();
				System.out.println(lastUpdatedByType.toString());
				assertTrue(lastUpdatedByType instanceof IntegerType);
				assertEquals(lastUpdatedBy.getLeft(), Types.INTEGER);

	}

	@Test
	public void test() {

		Ora2IcebergTypeMapper mapper = new Ora2IcebergTypeMapper(
				"number=decimal(37,11)", "%_ID:NUMBER=LONG;CURRENCY_AMOUNT:NUMBER=DECIMAL(21,2);ID:NUMBER=INTEGER");
		
		Pair<Integer, Type> invoiceId = mapper.icebergType("INVOICE_ID", java.sql.Types.NUMERIC, 0, 0);
		Type invoiceIdType = invoiceId.getRight();
		System.out.println(invoiceIdType.toString());
		assertTrue(invoiceIdType instanceof LongType);
		assertEquals(invoiceId.getLeft(), java.sql.Types.BIGINT);

		Pair<Integer, Type> currAmount = mapper.icebergType("CURRENCY_AMOUNT", java.sql.Types.NUMERIC, 0, 0);
		Type currAmountType = currAmount.getRight();
		System.out.println(currAmountType.toString());
		assertTrue(currAmountType instanceof DecimalType);
		assertEquals(currAmount.getLeft(), java.sql.Types.NUMERIC);
		DecimalType currAmountTypeDec = (DecimalType) currAmountType;
		assertEquals(currAmountTypeDec.scale(), 2);
		assertEquals(currAmountTypeDec.precision(), 21);

		Pair<Integer, Type> id = mapper.icebergType("ID", java.sql.Types.NUMERIC, 0, 0);
		Type idType = id.getRight();
		System.out.println(idType.toString());
		assertEquals(id.getLeft(), java.sql.Types.INTEGER);
		assertTrue(idType instanceof IntegerType);

	}

}
