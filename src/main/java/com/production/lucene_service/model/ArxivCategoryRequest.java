package com.production.lucene_service.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class ArxivCategoryRequest {

    @NotBlank(message = "Category is required (e.g. cs.AI, cs.CL)")
    private String category;

    @Min(value = 1, message = "maxResults must be at least 1")
    @Max(value = 50000, message = "maxResults must not exceed 50000")
    private int maxResults = 100;
}
