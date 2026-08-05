/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The default answer to {@link IndexCreationValidator#requiresMappings()} is what keeps R1.
 *
 * <p>{@code MetadataCreateIndexService} skips building a throwaway index service only when every registered
 * validator says it does not read the mapper service. A default of true means an implementation written
 * before that method existed -- which is every implementation outside this repository -- keeps the
 * temporary service and its own behaviour without being touched. A default of false would silently start
 * handing those implementations a null, and the failure would be a {@link NullPointerException} in
 * third-party code on a path they never opted into.
 *
 * <p>Small enough to look not worth writing, which is exactly why it is: the value is a one-word change away
 * from inverting the compatibility guarantee, and nothing else in the suite would notice.
 */
public class IndexCreationValidatorTests extends OpenSearchTestCase {

    /** A validator written the way every existing one is: implementing the one method the interface had. */
    private static final class ValidatorFromBeforeTheDefaultExisted implements IndexCreationValidator {
        @Override
        public void validate(MapperService mapperService, IndexSettings indexSettings) {}
    }

    public void testAValidatorThatSaysNothingIsGivenTheMapperService() {
        assertTrue(
            "an implementation that does not override requiresMappings must keep receiving a real mapper "
                + "service, or adding the method changed behaviour for code that never mentioned it",
            new ValidatorFromBeforeTheDefaultExisted().requiresMappings()
        );
    }

    public void testAValidatorCanDeclineTheMapperService() {
        IndexCreationValidator declines = new IndexCreationValidator() {
            @Override
            public boolean requiresMappings() {
                return false;
            }

            @Override
            public void validate(MapperService mapperService, IndexSettings indexSettings) {}
        };
        assertFalse("overriding the default has to be what actually opts in", declines.requiresMappings());
    }
}
