/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This strategy can be used by the Rest APIs wanting to paginate the responses based on Indices.
 * The strategy considers create timestamps of indices as the keys to iterate over pages.
 *
 * @opensearch.internal
 */
public class IndexPaginationStrategy implements PaginationStrategy<String> {
    private static final String DEFAULT_INDICES_PAGINATED_ENTITY = "indices";

    protected static final Comparator<IndexMetadata> ASC_COMPARATOR = (metadata1, metadata2) -> {
        if (metadata1.getCreationDate() == metadata2.getCreationDate()) {
            return metadata1.getIndex().getName().compareTo(metadata2.getIndex().getName());
        }
        return Long.compare(metadata1.getCreationDate(), metadata2.getCreationDate());
    };
    protected static final Comparator<IndexMetadata> DESC_COMPARATOR = (metadata1, metadata2) -> {
        if (metadata1.getCreationDate() == metadata2.getCreationDate()) {
            return metadata2.getIndex().getName().compareTo(metadata1.getIndex().getName());
        }
        return Long.compare(metadata2.getCreationDate(), metadata1.getCreationDate());
    };

    private final PageToken pageToken;
    private final List<String> requestedIndices;

    public IndexPaginationStrategy(PageParams pageParams, ClusterState clusterState) {

        IndexStrategyToken requestedToken = Objects.isNull(pageParams.getRequestedToken()) || pageParams.getRequestedToken().isEmpty()
            ? null
            : new IndexStrategyToken(pageParams.getRequestedToken());
        // Get list of indices metadata sorted by their creation time and filtered by the last sent index
        List<IndexMetadata> sortedIndices = getEligibleIndices(
            clusterState,
            pageParams.getSort(),
            Objects.isNull(requestedToken) ? null : requestedToken.lastIndexName,
            Objects.isNull(requestedToken) ? null : requestedToken.lastIndexCreationTime
        );

        // Trim sortedIndicesList to get the list of indices metadata to be sent as response
        List<IndexMetadata> metadataSublist = getMetadataSubList(sortedIndices, pageParams.getSize());
        // Get list of index names from the trimmed metadataSublist
        List<String> clusterStateNames = metadataSublist.stream()
            .map(metadata -> metadata.getIndex().getName())
            .collect(Collectors.toList());
        this.requestedIndices = mergeGatedIndices(
            clusterStateNames,
            metadataSublist,
            pageParams,
            Objects.isNull(requestedToken) ? null : requestedToken.lastIndexName,
            Objects.isNull(requestedToken) ? 0L : requestedToken.lastIndexCreationTime
        );
        this.pageToken = getResponseToken(
            pageParams.getSize(),
            sortedIndices.size(),
            metadataSublist.isEmpty() ? null : metadataSublist.get(metadataSublist.size() - 1)
        );
    }

    /**
     * Folds one page of gated indices into the page built from cluster state.
     *
     * <p>H16 recorded why this is a merge rather than a bigger sort. A gated index is not in
     * {@code metadata().indices()} at all, so the page was silently missing it, and the repair anyone
     * reaches for first, adding gated indices to the existing sort, would have made the cost problem worse:
     * that sort already walks the whole population to produce one page.
     *
     * <p>What makes a merge affordable is an asymmetry the design guarantees rather than hopes for. The
     * cluster state side is bounded by the number of <em>ordinary</em> indices, which is small by
     * construction because the massive population is exactly the part that is gated. The gated side is
     * bounded by the page size, because a pager is asked for a page. So the work is ordinary-count plus
     * page-size, with the hundred million appearing in neither term.
     *
     * <p>With no pager installed this returns the cluster state page unchanged, so a cluster that has never
     * gated an index behaves exactly as before.
     */
    private static List<String> mergeGatedIndices(
        List<String> clusterStateNames,
        List<IndexMetadata> clusterStatePage,
        PageParams pageParams,
        String lastIndexName,
        long lastIndexCreationTime
    ) {
        if (AbsentIndexDescriptorSuppliers.isPagerRegistered() == false) {
            return clusterStateNames;
        }
        boolean ascending = PageParams.PARAM_ASC_SORT_VALUE.equals(pageParams.getSort());
        List<AbsentIndexDescriptorSuppliers.PagedIndex> gatedPage = AbsentIndexDescriptorSuppliers.page(
            lastIndexName,
            lastIndexCreationTime,
            ascending,
            pageParams.getSize()
        );
        if (gatedPage.isEmpty()) {
            return clusterStateNames;
        }

        // Both sides are already in page order, so this is a merge of two sorted runs rather than a sort of
        // their union. Sorting the union would reintroduce the cost this exists to avoid.
        List<Sortable> merged = new ArrayList<>(clusterStatePage.size() + gatedPage.size());
        for (IndexMetadata metadata : clusterStatePage) {
            merged.add(new Sortable(metadata.getIndex().getName(), metadata.getCreationDate()));
        }
        for (AbsentIndexDescriptorSuppliers.PagedIndex gated : gatedPage) {
            merged.add(new Sortable(gated.name(), gated.creationDate()));
        }
        merged.sort(ascending ? SORTABLE_ASC : SORTABLE_DESC);

        List<String> page = new ArrayList<>(pageParams.getSize());
        for (Sortable each : merged) {
            if (page.size() >= pageParams.getSize()) {
                break;
            }
            page.add(each.name);
        }
        return page;
    }

    /** One entry's position in page order, which is all the merge needs from either side. */
    private static final class Sortable {
        private final String name;
        private final long creationDate;

        private Sortable(String name, long creationDate) {
            this.name = name;
            this.creationDate = creationDate;
        }
    }

    private static final Comparator<Sortable> SORTABLE_ASC = (a, b) -> a.creationDate == b.creationDate
        ? a.name.compareTo(b.name)
        : Long.compare(a.creationDate, b.creationDate);

    private static final Comparator<Sortable> SORTABLE_DESC = (a, b) -> a.creationDate == b.creationDate
        ? b.name.compareTo(a.name)
        : Long.compare(b.creationDate, a.creationDate);

    private static List<IndexMetadata> getEligibleIndices(
        ClusterState clusterState,
        String sortOrder,
        String lastIndexName,
        Long lastIndexCreationTime
    ) {
        if (Objects.isNull(lastIndexName) || Objects.isNull(lastIndexCreationTime)) {
            return PaginationStrategy.getSortedIndexMetadata(
                clusterState,
                PageParams.PARAM_ASC_SORT_VALUE.equals(sortOrder) ? ASC_COMPARATOR : DESC_COMPARATOR
            );
        } else {
            return PaginationStrategy.getSortedIndexMetadata(
                clusterState,
                getMetadataFilter(sortOrder, lastIndexName, lastIndexCreationTime),
                PageParams.PARAM_ASC_SORT_VALUE.equals(sortOrder) ? ASC_COMPARATOR : DESC_COMPARATOR
            );
        }
    }

    private static Predicate<IndexMetadata> getMetadataFilter(String sortOrder, String lastIndexName, Long lastIndexCreationTime) {
        if (Objects.isNull(lastIndexName) || Objects.isNull(lastIndexCreationTime)) {
            return indexMetadata -> true;
        }
        return getIndexCreateTimeFilter(sortOrder, lastIndexName, lastIndexCreationTime);
    }

    protected static Predicate<IndexMetadata> getIndexCreateTimeFilter(String sortOrder, String lastIndexName, Long lastIndexCreationTime) {
        boolean isAscendingSort = sortOrder.equals(PageParams.PARAM_ASC_SORT_VALUE);
        return metadata -> {
            if (metadata.getCreationDate() == lastIndexCreationTime) {
                return isAscendingSort
                    ? metadata.getIndex().getName().compareTo(lastIndexName) > 0
                    : metadata.getIndex().getName().compareTo(lastIndexName) < 0;
            }
            return isAscendingSort
                ? metadata.getCreationDate() > lastIndexCreationTime
                : metadata.getCreationDate() < lastIndexCreationTime;
        };
    }

    private List<IndexMetadata> getMetadataSubList(List<IndexMetadata> sortedIndices, final int pageSize) {
        if (sortedIndices.isEmpty()) {
            return new ArrayList<>();
        }
        return sortedIndices.subList(0, Math.min(pageSize, sortedIndices.size()));
    }

    private PageToken getResponseToken(final int pageSize, final int totalIndices, IndexMetadata lastIndex) {
        if (totalIndices <= pageSize) {
            return new PageToken(null, DEFAULT_INDICES_PAGINATED_ENTITY);
        }
        return new PageToken(
            new IndexStrategyToken(lastIndex.getCreationDate(), lastIndex.getIndex().getName()).generateEncryptedToken(),
            DEFAULT_INDICES_PAGINATED_ENTITY
        );
    }

    @Override
    public PageToken getResponseToken() {
        return pageToken;
    }

    @Override
    public List<String> getRequestedEntities() {
        return Objects.isNull(requestedIndices) ? new ArrayList<>() : requestedIndices;
    }

    /**
     * TokenParser to be used by {@link IndexPaginationStrategy}.
     * Token would look like: CreationTimeOfLastRespondedIndex + | + NameOfLastRespondedIndex
     */
    public static class IndexStrategyToken {

        private static final String JOIN_DELIMITER = "|";
        private static final String SPLIT_REGEX = "\\|";
        private static final int CREATE_TIME_POS_IN_TOKEN = 0;
        private static final int INDEX_NAME_POS_IN_TOKEN = 1;

        /**
         * Represents creation times of last index which was displayed in the page.
         * Used to identify the new start point in case the indices get created/deleted while queries are executed.
         */
        private final long lastIndexCreationTime;

        /**
         * Represents name of the last index which was displayed in the page.
         * Used to identify whether the sorted list of indices has changed or not.
         */
        private final String lastIndexName;

        public IndexStrategyToken(String requestedTokenString) {
            // TODO: Avoid validating the requested token multiple times while calling from Rest and/or Transport layer.
            validateIndexStrategyToken(requestedTokenString);
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split(SPLIT_REGEX);
            this.lastIndexCreationTime = Long.parseLong(decryptedTokenElements[CREATE_TIME_POS_IN_TOKEN]);
            this.lastIndexName = decryptedTokenElements[INDEX_NAME_POS_IN_TOKEN];
        }

        public IndexStrategyToken(long creationTimeOfLastRespondedIndex, String nameOfLastRespondedIndex) {
            Objects.requireNonNull(nameOfLastRespondedIndex, "index name should be provided");
            this.lastIndexCreationTime = creationTimeOfLastRespondedIndex;
            this.lastIndexName = nameOfLastRespondedIndex;
        }

        public String generateEncryptedToken() {
            return PaginationStrategy.encryptStringToken(String.join(JOIN_DELIMITER, String.valueOf(lastIndexCreationTime), lastIndexName));
        }

        /**
         * Will perform simple validations on token received in the request.
         * Token should be base64 encoded, and should contain the expected number of elements separated by "|".
         * Timestamps should also be a valid long.
         *
         * @param requestedTokenStr string denoting the encoded token requested by the user.
         */
        public static void validateIndexStrategyToken(String requestedTokenStr) {
            Objects.requireNonNull(requestedTokenStr, "requestedTokenString can not be null");
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenStr);
            final String[] decryptedTokenElements = decryptedToken.split(SPLIT_REGEX);
            if (decryptedTokenElements.length != 2) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
            try {
                long creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[CREATE_TIME_POS_IN_TOKEN]);
                if (creationTimeOfLastRespondedIndex <= 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }
    }

}
