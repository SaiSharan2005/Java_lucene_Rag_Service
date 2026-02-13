package com.production.lucene_service.model;

import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

import java.util.List;

@Data
public class ArxivPaperIdsRequest {

    @NotEmpty(message = "At least one paper ID is required")
    private List<String> paperIds;
}
